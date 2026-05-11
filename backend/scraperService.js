const { DateTime } = require('luxon');
const fs = require('fs');
const path = require('path');
const http = require('http');
const https = require('https');

class ScraperService {
  constructor(db, snapshotsDir, broadcast, videoGeneratorCallback) {
    this.db = db;
    this.snapshotsDir = snapshotsDir;
    this.broadcast = broadcast;
    this.videoGeneratorCallback = videoGeneratorCallback;
    this.activeScrapes = new Map();
    this.completedScrapes = new Map(); // Keep last 10 completed/failed sessions
  }

  async runScrape(config) {
    const {
      sessionId,
      camera,
      startIso,
      endIso,
      intervalSeconds,
      frigateApiUrl = process.env.FRIGATE_API_URL || 'http://frigate:5000',
      timezone = 'America/New_York'
    } = config;

    console.log(`Starting scrape for session ${sessionId}, camera: ${camera}`);

    const session = this.db.getSession(sessionId);
    if (!session) {
      throw new Error(`Session ${sessionId} not found`);
    }

    const sessionDir = path.join(this.snapshotsDir, sessionId);
    if (!fs.existsSync(sessionDir)) {
      fs.mkdirSync(sessionDir, { recursive: true });
    }

    const startTime = DateTime.fromISO(startIso, { zone: timezone });
    const endTime = DateTime.fromISO(endIso, { zone: timezone });

    // Normalize camera name: lowercase and replace spaces with underscores
    const normalizedCamera = camera.toLowerCase().replace(/\s+/g, '_');

    let frameCount = 0;
    const scrapeState = {
      active: true,
      totalFrames: 0,
      completedFrames: 0,
      startTime: Date.now()
    };

    this.activeScrapes.set(sessionId, scrapeState);

    // Calculate all frame timestamps first
    const framesToDownload = [];
    for (let currentTime = startTime; currentTime <= endTime; currentTime = currentTime.plus({ seconds: intervalSeconds })) {
      const timestamp = Math.floor(currentTime.toSeconds());
      const frameNumber = String(frameCount + 1).padStart(4, '0');
      framesToDownload.push({
        frameNumber,
        timestamp,
        currentTime: currentTime.toISO()
      });
      frameCount++;
    }

    scrapeState.totalFrames = frameCount;

    // Update database with total frames
    this.db.updateSession(sessionId, {
      progress_total: frameCount
    });

    console.log(`[Scraper] Starting parallel download of ${frameCount} frames with concurrency limit of 10`);

    const CONCURRENCY_LIMIT = 10;
    let completedCount = 0;

    try {
      // Process frames in batches
      for (let i = 0; i < framesToDownload.length; i += CONCURRENCY_LIMIT) {
        if (!scrapeState.active) {
          console.log(`Scrape for session ${sessionId} was stopped`);
          break;
        }

        const batch = framesToDownload.slice(i, i + CONCURRENCY_LIMIT);

        // Process batch in parallel
        await Promise.all(batch.map(async (frame) => {
          const { frameNumber, timestamp, currentTime } = frame;
          const snapshotFile = path.join(sessionDir, `frame_${frameNumber}.jpg`);

          // Define search window: +/- 5% of interval
          const windowSize = Math.max(1, Math.floor(intervalSeconds * 0.05));

          // Spiral search: T, T+1, T-1, T+2, T-2, ... up to window limit
          let downloaded = false;
          let usedTimestamp = timestamp;

          for (let offset = 0; offset <= windowSize; offset++) {
            const offsets = offset === 0 ? [0] : [offset, -offset];

            for (const tryOffset of offsets) {
              const tryTimestamp = timestamp + tryOffset;
              const tryUrl = `${frigateApiUrl}/api/${normalizedCamera}/recordings/${tryTimestamp}/snapshot.jpg`;

              try {
                await this.downloadSnapshot(tryUrl, snapshotFile);

                const relativePath = `/snapshots/${sessionId}/frame_${frameNumber}.jpg`;
                const stats = fs.statSync(snapshotFile);

                this.db.addSnapshot(sessionId, relativePath, {
                  file_size: stats.size,
                  captured_at: currentTime
                });

                scrapeState.completedFrames++;
                completedCount++;

                // Update database with progress in real-time using atomic frame number
                // Use the frame number (1-based index) to avoid race conditions from concurrent updates
                this.db.updateSession(sessionId, {
                  progress_current: parseInt(frameNumber)
                });

                this.broadcast({
                  type: 'snapshot',
                  sessionId: sessionId,
                  snapshot: relativePath,
                  count: completedCount,
                  scrape: true,
                  progress: {
                    completed: scrapeState.completedFrames,
                    total: scrapeState.totalFrames,
                    current: timestamp
                  }
                });

                if (tryOffset === 0) {
                  console.log(`Captured frame ${frameNumber} for session ${sessionId} at ${timestamp}`);
                } else {
                  console.log(`Captured frame ${frameNumber} for session ${sessionId} at ${timestamp} (found at ${tryTimestamp}, offset ${tryOffset}s)`);
                }
                downloaded = true;
                usedTimestamp = tryTimestamp;
                break;
              } catch (error) {
                if (offset === 0 && tryOffset === 0) {
                  console.error(`Failed to capture frame at ${timestamp}`);
                  console.error(`Failed URL: ${tryUrl}`);
                  console.error(`Error: ${error.message} - starting spiral search (window: +/-${windowSize}s)`);
                }
              }
            }

            if (downloaded) break;
          }

          if (!downloaded) {
            console.error(`Skipping frame ${frameNumber} for session ${sessionId} - no valid frame found in window +/-${windowSize}s`);
          }
        }));

        console.log(`[Scraper] Completed batch ${Math.floor(i / CONCURRENCY_LIMIT) + 1}/${Math.ceil(framesToDownload.length / CONCURRENCY_LIMIT)} (${completedCount}/${frameCount} frames)`);
      }

      console.log(`[Scraper] Finished all frames. Captured ${completedCount} frames. Emitting/Triggering render for session: ${sessionId}`);

      // Trigger video generation after scrape is complete
      if (completedCount > 0) {
        console.log(`[Scraper] Calling triggerVideoGeneration for session: ${sessionId}`);
        await this.triggerVideoGeneration(sessionId);
      }

      return {
        success: true,
        framesCaptured: completedCount,
        sessionId: sessionId
      };

    } catch (error) {
      console.error(`Scrape failed for session ${sessionId}:`, error.message);

      // Mark as failed in completed scrapes
      this.completedScrapes.set(sessionId, {
        ...scrapeState,
        active: false,
        completed: false,
        failed: true,
        completedAt: Date.now()
      });

      throw error;

    } finally {
      // Only move to completed scrapes if not already set by catch block
      if (!this.completedScrapes.has(sessionId)) {
        this.completedScrapes.set(sessionId, {
          ...scrapeState,
          active: false,
          completed: true,
          failed: false,
          completedAt: Date.now()
        });
      }

      // Keep only last 10 completed sessions
      if (this.completedScrapes.size > 10) {
        const oldestKey = this.completedScrapes.keys().next().value;
        this.completedScrapes.delete(oldestKey);
      }

      this.activeScrapes.delete(sessionId);

      // Mark session as completed
      this.db.updateSession(sessionId, {
        active: 0,
        completed_at: new Date().toISOString()
      });
    }
  }

  async downloadSnapshot(url, outputPath) {
    return new Promise((resolve, reject) => {
      try {
        const urlObj = new URL(url);
        const client = urlObj.protocol === 'https:' ? https : http;

        const req = client.request(url, (res) => {
          if (res.statusCode === 404 || res.statusCode === 422) {
            reject(new Error(`HTTP ${res.statusCode}: ${res.statusMessage}`));
            return;
          }

          if (res.statusCode !== 200) {
            reject(new Error(`HTTP ${res.statusCode}: ${res.statusMessage}`));
            return;
          }

          const fileStream = fs.createWriteStream(outputPath);
          res.pipe(fileStream);

          fileStream.on('finish', () => {
            fileStream.close();

            // Verify file exists and has content
            if (fs.existsSync(outputPath)) {
              const stats = fs.statSync(outputPath);
              if (stats.size > 0) {
                resolve(outputPath);
              } else {
                fs.unlinkSync(outputPath);
                reject(new Error('Downloaded file is empty'));
              }
            } else {
              reject(new Error('Failed to create output file'));
            }
          });

          fileStream.on('error', (error) => {
            reject(error);
          });
        });

        req.on('error', (error) => {
          reject(error);
        });

        req.setTimeout(10000, () => {
          req.destroy();
          reject(new Error('Request timeout'));
        });

        req.end();
      } catch (error) {
        reject(new Error(`Invalid URL: ${error.message}`));
      }
    });
  }

  async triggerVideoGeneration(sessionId) {
    console.log(`[Scraper] Triggering video generation for scraped session ${sessionId}`);
    console.log(`[Scraper] FFmpeg video rendering will start shortly for session ${sessionId}...`);

    // Call the video generation callback if available
    if (this.videoGeneratorCallback) {
      console.log(`[Scraper] Calling video generation callback for session: ${sessionId}`);
      try {
        await this.videoGeneratorCallback(sessionId);
      } catch (error) {
        console.error(`[Scraper] Error calling video generation callback:`, error);
      }
    } else {
      console.warn(`[Scraper] No video generation callback available, broadcasting instead`);
      // Fallback to broadcast for backward compatibility
      this.broadcast({
        type: 'scrape_complete',
        sessionId: sessionId,
        message: 'Scraping completed, ready for video generation'
      });
    }
  }

  stopScrape(sessionId) {
    const scrapeState = this.activeScrapes.get(sessionId);
    if (scrapeState) {
      scrapeState.active = false;
      console.log(`Stopping scrape for session ${sessionId}`);

      // Update database to mark session as cancelled
      this.db.updateSession(sessionId, {
        active: 0,
        completed_at: new Date().toISOString()
      });

      return true;
    }
    return false;
  }

  getScrapeStatus(sessionId) {
    // Get session from database to get progress fields
    const session = this.db.getSession(sessionId);

    // Check active scrapes first
    let scrapeState = this.activeScrapes.get(sessionId);
    if (scrapeState) {
      return {
        active: scrapeState.active,
        completed: false,
        failed: false,
        completedFrames: scrapeState.completedFrames,
        totalFrames: scrapeState.totalFrames,
        progress_current: session?.progress_current || scrapeState.completedFrames,
        progress_total: session?.progress_total || scrapeState.totalFrames,
        elapsedMs: Date.now() - scrapeState.startTime
      };
    }

    // Check completed scrapes (last 10)
    scrapeState = this.completedScrapes.get(sessionId);
    if (scrapeState) {
      return {
        active: false,
        completed: scrapeState.completed || false,
        failed: scrapeState.failed || false,
        completedFrames: scrapeState.completedFrames,
        totalFrames: scrapeState.totalFrames,
        progress_current: session?.progress_current || scrapeState.completedFrames,
        progress_total: session?.progress_total || scrapeState.totalFrames,
        elapsedMs: scrapeState.completedAt - scrapeState.startTime,
        videoUrl: scrapeState.completed ? `/output/${sessionId}.mp4` : null
      };
    }

    return null;
  }

  async getFrigateCameras(apiUrl) {
    return new Promise((resolve, reject) => {
      try {
        const url = new URL(`${apiUrl}/api/config`);
        const isHttps = url.protocol === 'https:';
        const client = isHttps ? https : http;
        
        const options = {
          hostname: url.hostname,
          port: url.port || (isHttps ? 443 : 80),
          path: '/api/config',
          method: 'GET'
        };

        const req = client.request(options, (res) => {
          let data = '';

          res.on('data', (chunk) => {
            data += chunk;
          });

          res.on('end', () => {
            try {
              if (res.statusCode !== 200) {
                reject(new Error(`Frigate API returned status ${res.statusCode}`));
                return;
              }

              const config = JSON.parse(data);
              const cameras = [];

              if (config.cameras) {
                for (const [cameraName, cameraConfig] of Object.entries(config.cameras)) {
                  if (cameraConfig.enabled !== false) {
                    cameras.push({
                      name: cameraName,
                      displayName: cameraConfig.display_name || cameraName,
                      enabled: cameraConfig.enabled !== false
                    });
                  }
                }
              }

              resolve(cameras);
            } catch (parseError) {
              reject(new Error(`Failed to parse Frigate config: ${parseError.message}`));
            }
          });
        });

        req.on('error', (error) => {
          reject(new Error(`Failed to connect to Frigate API: ${error.message}`));
        });

        req.setTimeout(10000, () => {
          req.destroy();
          reject(new Error('Frigate API request timeout'));
        });

        req.end();
      } catch (error) {
        reject(new Error(`Invalid Frigate API URL: ${error.message}`));
      }
    });
  }

  calculatePreview(startIso, endIso, intervalSeconds) {
    const startTime = DateTime.fromISO(startIso);
    const endTime = DateTime.fromISO(endIso);
    const duration = endTime.diff(startTime, 'seconds').seconds;
    const totalFrames = Math.ceil(duration / intervalSeconds);
    const estimatedVideoDuration = totalFrames / 30; // Assuming 30 FPS for output

    return {
      totalFrames,
      estimatedVideoDuration: Math.round(estimatedVideoDuration),
      durationSeconds: Math.round(duration)
    };
  }
}

module.exports = ScraperService;
