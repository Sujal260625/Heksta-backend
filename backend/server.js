const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');
const ip = require('ip');
const { v4: uuidv4 } = require('uuid');
const QRCode = require('qrcode');
const multer = require('multer');
const archiver = require('archiver');
require('dotenv').config(); // Load environment variables

const app = express();
const server = http.createServer(app);

// Increase timeout for massive file transfers (50GB support)
server.timeout = 0; // Disable timeout for long uploads
server.headersTimeout = 0;
server.keepAliveTimeout = 600000; // 10 minutes keep-alive
server.requestTimeout = 0; // Modern node support

// Configure Multer once at top level to optimize memory and handle potential massive uploads
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    const sid = req.params.sessionId;
    const dir = path.join(__dirname, 'uploads', sid);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    cb(null, dir);
  },
  filename: (req, file, cb) => {
    // Obfuscate filenames on disk for user privacy
    cb(null, `${Date.now()}-${uuidv4()}${path.extname(file.originalname)}`);
  }
});

const uploadMiddleware = multer({
  storage: storage,
  limits: {
    fileSize: 2 * 1024 * 1024 * 1024, // 2GB limit
    fieldSize: 1024 * 1024 * 1024 // 1GB field size
  }
}).array('files');

// Initialize WebSocket Servers with noServer: true to handle routing manually
const wss = new WebSocket.Server({ noServer: true });
const whisperWss = new WebSocket.Server({ noServer: true });

const PORT = process.env.PORT || 5000;
const CLIENT_URL = process.env.CLIENT_URL;

// Get server URL
let cachedServerUrl = null;
const getServerUrl = () => {
  if (cachedServerUrl) return cachedServerUrl;

  if (process.env.NODE_ENV === 'production') {
    cachedServerUrl = 'https://heksta-backend-final.onrender.com';
  } else {
    cachedServerUrl = `http://${getLocalIP()}:${PORT}`;
  }
  return cachedServerUrl;
};

// Store active sessions
const sessions = new Map();
const downloadStats = new Map();
const whisperUsers = new Map(); // { clientId: { name, ws, joinedAt } }
const whisperFiles = new Map(); // { fileId: { path, originalName, mimetype, uploadedBy, uploadedAt } }

const cors = require('cors');

// Middleware
app.use(express.json());

// CORS Configuration
app.use(cors({
  origin: [CLIENT_URL, 'https://heksta.in', 'https://heksta-backend-final.onrender.com'].filter(Boolean),
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Origin', 'X-Requested-With', 'Content-Type', 'Accept']
}));

// ── Parallel Resumable Upload Tracking ──
// Tracks partial uploads per session per file
// Each file is split into chunks; each chunk stored as individual temp file.
// When all chunks arrive, they are concatenated in order and finalized.
const partialUploads = new Map(); // key: `${sessionId}:${fileId}` -> { path, totalBytes, fileName, receivedChunks (Set), totalChunks, updatedAt, sessionId, fileId }

// Get local IP
const getLocalIP = () => {
  return ip.address();
};

// Generate session ID
const generateSessionId = () => {
  return uuidv4().substring(0, 8);
};

// Create file session
app.post('/api/create-session', (req, res) => {
  try {
    const sessionId = generateSessionId();
    const { password = '', senderName = 'Anonymous' } = req.body;

    const session = {
      id: sessionId,
      password,
      senderName,
      files: [],
      socketFiles: [], // Track files shared via WebSockets
      createdAt: Date.now(),
      active: true,
      connectedClients: 0
    };

    sessions.set(sessionId, session);

    // Generate both Local and Global links
    const localIp = getLocalIP();
    const isProd = process.env.NODE_ENV === 'production';

    const globalBase = isProd ? 'https://heksta.in' : `http://${localIp}:${PORT}`;
    const localBase = `http://${localIp}:${PORT}`;

    res.json({
      sessionId,
      joinLink: `${globalBase}/join/${sessionId}`,
      localJoinLink: `${localBase}/join/${sessionId}`,
      serverUrl: globalBase,
      localServerUrl: localBase
    });
  } catch (error) {
    console.error('Session creation error:', error);
    res.status(500).json({ error: 'Failed to create session: ' + error.message });
  }
});

// Upload files to session
app.post('/api/upload/:sessionId', (req, res) => {
  const sessionId = req.params.sessionId;
  const session = sessions.get(sessionId);

  if (!session) {
    return res.status(404).json({ error: 'Session not found' });
  }

  if (!session.active) {
    return res.status(400).json({ error: 'Session is not active' });
  }

  uploadMiddleware(req, res, (err) => {
    if (err) {
      console.error('Upload error:', err);
      if (err.code === 'LIMIT_FILE_SIZE') {
        return res.status(400).json({ error: 'no greater then 2 GB file' });
      }
      return res.status(500).json({ error: 'Upload failed: ' + err.message });
    }

    // Check if files were uploaded
    if (!req.files || req.files.length === 0) {
      return res.status(400).json({ error: 'No files uploaded' });
    }

    const files = req.files.map(file => ({
      id: uuidv4(),
      originalName: file.originalname,  // multer captures the 3rd arg of formData.append here
      filename: file.filename,
      path: file.path,
      size: file.size,
      mimetype: file.mimetype,
      uploadedAt: Date.now()
    }));

    session.files = [...session.files, ...files];
    sessions.set(sessionId, session);

    console.log(`Uploaded ${files.length} files to session ${sessionId}`);

    // Notify connected clients
    const filesList = [
      ...(session.files.map(f => ({
        id: f.id,
        name: f.originalName,
        size: f.size,
        type: f.mimetype
      }))),
      ...(session.socketFiles || [])
    ];

    broadcastToSession(sessionId, {
      type: 'files_updated',
      files: filesList
    });

    res.json({
      message: 'Files uploaded successfully',
      files: files.map(f => ({
        id: f.id,
        name: f.originalName,
        size: f.size
      }))
    });
  });
});

// ── Parallel Upload: Get upload status ──
app.get('/api/upload/status/:sessionId/:fileId', (req, res) => {
  const { sessionId, fileId } = req.params;
  const key = `${sessionId}:${fileId}`;
  const partial = partialUploads.get(key);

  if (!partial) {
    const session = sessions.get(sessionId);
    if (session) {
      const existing = session.files.find(f => f.id === fileId);
      if (existing) {
        return res.json({ uploadedBytes: existing.size, totalBytes: existing.size, complete: true });
      }
    }
    return res.json({ uploadedBytes: 0, totalBytes: 0, complete: false, receivedChunks: [] });
  }

  res.json({
    uploadedBytes: partial.receivedChunks.size * partial.chunkSize,
    totalBytes: partial.totalBytes,
    complete: false,
    fileName: partial.fileName,
    totalChunks: partial.totalChunks,
    receivedChunks: Array.from(partial.receivedChunks),
    chunkSize: partial.chunkSize
  });
});

// ── Parallel Upload: Upload a single chunk ──
// Chunks arrive in parallel and may be out of order.
// Each chunk is saved as a separate temp file: .part_${fileId}_${chunkIndex}
// When ALL chunks arrive, they are concatenated in order and finalized.
app.post('/api/upload/chunk/:sessionId/:fileId', (req, res) => {
  const { sessionId, fileId } = req.params;
  const key = `${sessionId}:${fileId}`;

  const chunkSize = parseInt(req.headers['x-chunk-size'] || '0', 10);
  const totalBytes = parseInt(req.headers['x-total-size'] || '0', 10);
  const fileName = req.headers['x-file-name'] || 'unknown';
  const chunkIndex = parseInt(req.headers['x-chunk-index'] || '0', 10);
  const totalChunks = parseInt(req.headers['x-total-chunks'] || '1', 10);

  if (!chunkSize || !totalBytes || totalChunks <= 0) {
    return res.status(400).json({ error: 'Missing required chunk headers' });
  }

  const uploadsDir = path.join(__dirname, 'uploads', sessionId);
  if (!fs.existsSync(uploadsDir)) {
    fs.mkdirSync(uploadsDir, { recursive: true });
  }

  // Initialize or get partial upload tracker
  if (!partialUploads.has(key)) {
    partialUploads.set(key, {
      totalBytes,
      fileName,
      sessionId,
      fileId,
      totalChunks,
      chunkSize,
      receivedChunks: new Set(),
      updatedAt: Date.now()
    });
  }

  const partial = partialUploads.get(key);
  partial.updatedAt = Date.now();

  // Save chunk as individual temp file (out-of-order safe)
  const chunkFilePath = path.join(uploadsDir, `.part_${fileId}_${chunkIndex}`);
  let dataBuffer = Buffer.alloc(0);

  req.on('data', (chunk) => {
    dataBuffer = Buffer.concat([dataBuffer, chunk]);
  });

  req.on('end', () => {
    try {
      // Write chunk to its own temp file
      fs.writeFileSync(chunkFilePath, dataBuffer);
      partial.receivedChunks.add(chunkIndex);
      partialUploads.set(key, partial);

      const uploadedBytes = partial.receivedChunks.size * chunkSize;
      const isComplete = partial.receivedChunks.size >= totalChunks;
      const remainingChunks = totalChunks - partial.receivedChunks.size;

      console.log(`[ParallelUpload] Chunk ${chunkIndex}/${totalChunks} for ${fileName} (${remainingChunks} remaining)`);

      if (isComplete) {
        // ── ALL CHUNKS RECEIVED — Concatenate in order ──
        console.log(`[ParallelUpload] All ${totalChunks} chunks received for ${fileName} — concatenating...`);

        const finalFileName = `${Date.now()}-${uuidv4()}${path.extname(fileName)}`;
        const finalPath = path.join(uploadsDir, finalFileName);
        const writeStream = fs.createWriteStream(finalPath);

        let concatIndex = 0;
        const concatNext = () => {
          if (concatIndex >= totalChunks) {
            writeStream.end();
            // Clean up chunk files
            for (let i = 0; i < totalChunks; i++) {
              const cp = path.join(uploadsDir, `.part_${fileId}_${i}`);
              try { if (fs.existsSync(cp)) fs.unlinkSync(cp); } catch (e) { /* ignore */ }
            }
            partialUploads.delete(key);

            // Register in session
            const session = sessions.get(sessionId);
            if (session) {
              const fileEntry = {
                id: fileId,
                originalName: fileName,
                filename: finalFileName,
                path: finalPath,
                size: totalBytes,
                mimetype: 'application/octet-stream',
                uploadedAt: Date.now()
              };
              session.files = [...session.files, fileEntry];
              sessions.set(sessionId, session);

              const filesList = session.files.map(f => ({
                id: f.id, name: f.originalName, size: f.size, type: f.mimetype
              }));
              broadcastToSession(sessionId, { type: 'files_updated', files: filesList });
              console.log(`[ParallelUpload] Upload complete for ${fileName} in session ${sessionId}`);
            }

            res.json({ uploadedBytes: totalBytes, totalBytes, complete: true });
            return;
          }

          const cp = path.join(uploadsDir, `.part_${fileId}_${concatIndex}`);
          if (fs.existsSync(cp)) {
            const readStream = fs.createReadStream(cp, { highWaterMark: 1024 * 1024 });
            readStream.pipe(writeStream, { end: false });
            readStream.on('end', () => {
              concatIndex++;
              concatNext();
            });
            readStream.on('error', (err) => {
              console.error(`[ParallelUpload] Concat error at chunk ${concatIndex}:`, err);
              concatIndex++;
              concatNext();
            });
          } else {
            // Missing chunk — shouldn't happen but handle gracefully
            console.error(`[ParallelUpload] Missing chunk ${concatIndex} for ${fileName}`);
            concatIndex++;
            concatNext();
          }
        };

        concatNext();
      } else {
        // Return current status — how many chunks received so far
        res.json({
          uploadedBytes,
          totalBytes,
          complete: false,
          receivedChunks: partial.receivedChunks.size,
          totalChunks
        });
      }
    } catch (err) {
      console.error(`[ParallelUpload] Chunk save error:`, err);
      res.status(500).json({ error: 'Failed to save chunk' });
    }
  });
});

// ── Parallel Upload: Cancel/Abort partial upload (cleanup all chunk files) ──
app.delete('/api/upload/chunk/:sessionId/:fileId', (req, res) => {
  const { sessionId, fileId } = req.params;
  const key = `${sessionId}:${fileId}`;
  const partial = partialUploads.get(key);

  if (partial) {
    for (let i = 0; i < (partial.totalChunks || 10000); i++) {
      const cp = path.join(__dirname, 'uploads', sessionId, `.part_${fileId}_${i}`);
      try { if (fs.existsSync(cp)) fs.unlinkSync(cp); } catch (e) { break; }
    }
    partialUploads.delete(key);
  }

  res.json({ message: 'Partial upload cancelled' });
});

// Get session info
app.get('/api/session/:sessionId', (req, res) => {
  const sessionId = req.params.sessionId;
  const session = sessions.get(sessionId);

  if (!session) {
    return res.status(404).json({ error: 'Session not found' });
  }

  res.json({
    sessionId: session.id,
    senderName: session.senderName,
    fileCount: session.files.length,
    connectedClients: session.connectedClients,
    requiresPassword: !!session.password
  });
});

// Verify session password
app.post('/api/session/:sessionId/verify', (req, res) => {
  const sessionId = req.params.sessionId;
  const { password } = req.body;
  const session = sessions.get(sessionId);

  if (!session) {
    return res.status(404).json({ error: 'Session not found' });
  }

  if (session.password && session.password !== password) {
    return res.status(401).json({ error: 'Invalid password' });
  }

  res.json({ verified: true });
});

// Get files list
app.get('/api/session/:sessionId/files', (req, res) => {
  const sessionId = req.params.sessionId;
  const session = sessions.get(sessionId);

  if (!session) {
    return res.status(404).json({ error: 'Session not found' });
  }

  const filesList = [
    ...(session.files.map(file => ({
      id: file.id,
      name: file.originalName,
      size: file.size,
      type: file.mimetype
    }))),
    ...(session.socketFiles || [])
  ];

  res.json({
    files: filesList
  });
});

// Stream file download
app.get('/api/download/:sessionId/:fileId', (req, res) => {
  const { sessionId, fileId } = req.params;
  const session = sessions.get(sessionId);

  if (!session) {
    console.log(`Download failed: Session ${sessionId} not found`);
    return res.status(404).json({ error: 'Session not found' });
  }

  const file = session.files.find(f => f.id === fileId);
  if (!file) {
    console.log(`Download failed: File ${fileId} not found in session ${sessionId}`);
    return res.status(404).json({ error: 'File not found' });
  }

  const filePath = file.path;

  // Check if file exists
  if (!fs.existsSync(filePath)) {
    console.log(`Download failed: File ID ${fileId} not found on server`);
    trackDownloadFailure(sessionId, fileId, 'File not found on server');
    return res.status(404).json({ error: 'File not found on server' });
  }

  // Track download start
  const clientId = req.query.clientId || 'unknown';
  trackDownloadStart(sessionId, fileId, clientId);
  // Optimize download stream with larger buffer for higher speed
  const stat = fs.statSync(filePath);
  const fileSize = stat.size;
  const range = req.headers.range;

  console.log(`Starting High-Speed Transfer: File ID ${fileId} (${fileSize} bytes)`);

  // Handle cleanup on response finish
  const cleanup = () => {
    trackDownloadComplete(sessionId, fileId);
  };

  const handleError = (error) => {
    console.error(`Download error for ${fileId}:`, error);
    trackDownloadFailure(sessionId, fileId, error.message || 'Unknown error');
  };

  res.on('finish', cleanup);
  res.on('error', handleError);

  if (range) {
    // Handle range requests for resume support
    const parts = range.replace(/bytes=/, "").split("-");
    const start = parseInt(parts[0], 10);
    const end = parts[1] ? parseInt(parts[1], 10) : fileSize - 1;
    const chunksize = (end - start) + 1;

    const fileStream = fs.createReadStream(filePath, { start, end, highWaterMark: 1024 * 1024 }); // 1MB buffer

    res.writeHead(206, {
      'Content-Range': `bytes ${start}-${end}/${fileSize}`,
      'Accept-Ranges': 'bytes',
      'Content-Length': chunksize,
      'Content-Type': file.mimetype || 'application/octet-stream',
      'Content-Disposition': `attachment; filename="${encodeURIComponent(file.originalName)}"`
    });

    // High-performance piping
    fileStream.pipe(res);
    fileStream.on('error', handleError);
  } else {
    // Full file download
    const fileStream = fs.createReadStream(filePath, { highWaterMark: 1024 * 1024 }); // 1MB buffer

    res.writeHead(200, {
      'Content-Length': fileSize,
      'Content-Type': file.mimetype || 'application/octet-stream',
      'Content-Disposition': `attachment; filename="${encodeURIComponent(file.originalName)}"`
    });

    fileStream.pipe(res);
    fileStream.on('error', handleError);
  }
});

// Download all files as ZIP
app.get('/api/download-all/:sessionId', (req, res) => {
  const { sessionId } = req.params;
  const session = sessions.get(sessionId);

  if (!session) {
    console.log(`Download all failed: Session ${sessionId} not found`);
    return res.status(404).json({ error: 'Session not found' });
  }

  if (!session.files || session.files.length === 0) {
    console.log(`Download all failed: No files in session ${sessionId}`);
    return res.status(400).json({ error: 'No files to download' });
  }

  console.log(`Starting download all for session ${sessionId} (${session.files.length} files)`);

  const archive = archiver('zip', {
    zlib: { level: 5 } // Balanced compression
  });

  // Set response headers
  res.attachment(`${session.senderName || 'heksta'}-all-files.zip`);

  archive.on('error', (err) => {
    console.error(`Archive error for session ${sessionId}:`, err);
    if (!res.headersSent) {
      res.status(500).send({ error: 'Failed to create zip archive: ' + err.message });
    }
  });

  archive.on('warning', (err) => {
    if (err.code === 'ENOENT') {
      console.warn(`Archive warning for session ${sessionId}:`, err);
    } else {
      console.error(`Archive error for session ${sessionId}:`, err);
      throw err;
    }
  });

  // Stream archive to response
  archive.pipe(res);

  // Add each file to the archive
  session.files.forEach(file => {
    if (fs.existsSync(file.path)) {
      archive.file(file.path, { name: file.originalName });
    } else {
      console.warn(`File not found for zip: File ID ${file.id}`);
    }
  });

  archive.finalize();
});

// Close session
app.post('/api/session/:sessionId/close', (req, res) => {
  const sessionId = req.params.sessionId;
  const session = sessions.get(sessionId);

  if (!session) {
    return res.status(404).json({ error: 'Session not found' });
  }

  session.active = false;
  sessions.set(sessionId, session);

  // Clean up files
  const uploadDir = path.join(__dirname, 'uploads', sessionId);
  if (fs.existsSync(uploadDir)) {
    fs.rmSync(uploadDir, { recursive: true, force: true });
  }

  // Notify clients
  broadcastToSession(sessionId, {
    type: 'session_closed'
  });

  res.json({ message: 'Session closed successfully' });
});

// GET status to verify deployments
app.get('/api/status', (req, res) => {
  res.json({ status: 'Heksta backend running 🚀', version: '2.0.3-leave-fix' });
});

// ── WHISPER MODE FILE UPLOAD ──
const whisperUpload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => {
      const dir = path.join(__dirname, 'uploads', '_whisper');
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      cb(null, dir);
    },
    filename: (req, file, cb) => cb(null, `${uuidv4()}-${Date.now()}${path.extname(file.originalname)}`)
  }),
  limits: { fileSize: 5 * 1024 * 1024 * 1024 } // 5GB limit for whispers
}).single('file');

app.post('/api/whisper/upload', (req, res) => {
  whisperUpload(req, res, (err) => {
    if (err) return res.status(500).json({ error: 'Upload failed: ' + err.message });
    if (!req.file) return res.status(400).json({ error: 'No file provided' });

    const fileId = uuidv4();
    whisperFiles.set(fileId, {
      path: req.file.path,
      originalName: req.file.originalname,
      mimetype: req.file.mimetype,
      size: req.file.size,
      uploadedBy: req.body.senderId || 'unknown',
      roomId: req.body.roomId || 'global',
      uploadedAt: Date.now()
    });

    // Auto-delete after 24 hours
    setTimeout(() => {
      const f = whisperFiles.get(fileId);
      if (f && fs.existsSync(f.path)) fs.unlinkSync(f.path);
      whisperFiles.delete(fileId);
    }, 24 * 60 * 60 * 1000);

    console.log(`Whisper file uploaded: File ID ${fileId} (${req.file.size} bytes)`);
    res.json({ fileId, fileName: req.file.originalname, fileSize: req.file.size, mimeType: req.file.mimetype });
  });
});

// Serve a whisper file
app.get('/api/whisper/file/:fileId', (req, res) => {
  const file = whisperFiles.get(req.params.fileId);
  if (!file || !fs.existsSync(file.path)) return res.status(404).json({ error: 'File not found' });

  res.setHeader('Content-Disposition', `inline; filename="${encodeURIComponent(file.originalName)}"`);
  res.setHeader('Content-Type', file.mimetype || 'application/octet-stream');
  res.setHeader('Content-Length', file.size);
  fs.createReadStream(file.path).pipe(res);
});


server.on('upgrade', (request, socket, head) => {
  const url = new URL(request.url, `http://${request.headers.host}`);
  const pathname = url.pathname;

  if (pathname === '/whisper') {
    whisperWss.handleUpgrade(request, socket, head, (ws) => {
      whisperWss.emit('connection', ws, request);
    });
  } else if (pathname === '/') {
    // Main file-sharing websocket
    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit('connection', ws, request);
    });
  } else {
    // If it doesn't match our routes, destroy the socket to prevent hanging
    socket.destroy();
  }
});

// Main File Sharing WebSocket connection handling
wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const sessionId = url.searchParams.get('sessionId');
  const clientId = uuidv4();

  if (!sessionId || !sessions.has(sessionId)) {
    ws.close(1008, 'Invalid session');
    return;
  }

  const session = sessions.get(sessionId);
  session.connectedClients += 1;
  sessions.set(sessionId, session);

  ws.sessionId = sessionId;
  ws.clientId = clientId;

  // Send current session info
  ws.send(JSON.stringify({
    type: 'connected',
    clientId,
    sessionInfo: {
      sessionId: session.id,
      senderName: session.senderName,
      fileCount: session.files.length,
      connectedClients: session.connectedClients,
      files: [
        ...(session.files.map(f => ({
          id: f.id,
          name: f.originalName,
          size: f.size,
          type: f.mimetype
        }))),
        ...(session.socketFiles || [])
      ]
    }
  }));

  // Notify sender of new connection
  broadcastToSession(sessionId, {
    type: 'client_connected',
    clientId,
    connectedClients: session.connectedClients
  });

  ws.on('message', (data) => {
    try {
      const message = JSON.parse(data);

      switch (message.type) {
        case 'announce_file': {
          const { fileId, fileName, fileSize, fileType } = message;
          const session = sessions.get(sessionId);
          if (session) {
            if (!session.socketFiles) session.socketFiles = [];

            // Add to session socket files if not already there
            if (!session.socketFiles.find(f => f.id === fileId)) {
              session.socketFiles.push({
                id: fileId,
                name: fileName,
                size: fileSize,
                type: fileType,
                isSocketFile: true,
                announcedAt: Date.now()
              });

              // Notify others that a new file is being shared via socket
              broadcastToSession(sessionId, {
                type: 'file_announced',
                file: {
                  id: fileId,
                  name: fileName,
                  size: fileSize,
                  type: fileType,
                  isSocketFile: true
                }
              }, clientId);
            }
          }
          break;
        }
        case 'broadcast_file_chunk': {
          const { chunk, fileId, isLast, fileName, fileType, fileSize, chunkIndex, totalChunks } = message;
          // Relay to ALL OTHER clients in the session
          // We can relay binary data if chunk is an ArrayBuffer/Blob, 
          // but for JSON compatibility we might stick to base64 or send raw if it's a binary message.
          // For now, let's just relay whatever we got.
          broadcastToSession(sessionId, {
            type: 'broadcast_file_chunk',
            from: clientId,
            chunk,
            fileId,
            isLast,
            fileName,
            fileType,
            fileSize,
            chunkIndex,
            totalChunks,
            timestamp: Date.now()
          }, clientId); // Exclude sender
          break;
        }
        // ... existing main wss logic could follow if any
      }
    } catch (e) {
      // Not JSON or other error
    }
  });

  ws.on('close', () => {
    const session = sessions.get(sessionId);
    if (session) {
      session.connectedClients = Math.max(0, session.connectedClients - 1);
      sessions.set(sessionId, session);

      broadcastToSession(sessionId, {
        type: 'client_disconnected',
        clientId,
        connectedClients: session.connectedClients
      });
    }
  });

  ws.on('error', (error) => {
    console.error('WebSocket error:', error);
  });
});


// Whisper Mode WebSocket connection handling
// Heartbeat: every 15s ping all clients; remove any that don't pong back
const WHISPER_HEARTBEAT_INTERVAL = 15000;

const whisperHeartbeat = setInterval(() => {
  whisperWss.clients.forEach(wsClient => {
    if (wsClient.isAlive === false) {
      // Client didn't respond to last ping — terminate it
      wsClient.terminate();
      return;
    }
    wsClient.isAlive = false;
    wsClient.ping();
  });
}, WHISPER_HEARTBEAT_INTERVAL);

whisperWss.on('close', () => clearInterval(whisperHeartbeat));

whisperWss.on('connection', (ws, req) => {
  const clientId = uuidv4();
  ws.clientId = clientId;
  ws.isAlive = true; // Mark alive on connect

  // Respond to heartbeat pings
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (data) => {
    try {
      const message = JSON.parse(data);

      switch (message.type) {
        case 'join': {
          const userName = (message.name || 'Anonymous').trim().slice(0, 30);
          const roomId = message.roomId || 'global';
          whisperUsers.delete(clientId);
          whisperUsers.set(clientId, {
            id: clientId,
            name: userName,
            roomId,
            ws,
            joinedAt: Date.now()
          });
          // Send clientId back to the client
          ws.send(JSON.stringify({ type: 'connected', clientId }));
          console.log(`Whisper: User ID (${clientId}) joined room ${roomId}. Total: ${whisperUsers.size}`);
          broadcastWhisperUserList(roomId);
          break;
        }
        case 'private_message': {
          const { to, content } = message;
          const targetUser = whisperUsers.get(to);
          if (targetUser && targetUser.ws.readyState === WebSocket.OPEN) {
            targetUser.ws.send(JSON.stringify({
              type: 'private_message',
              from: clientId,
              fromName: whisperUsers.get(clientId)?.name || 'Unknown',
              content,
              timestamp: Date.now()
            }));
          }
          break;
        }
        case 'room_message': {
          const { content } = message;
          const user = whisperUsers.get(clientId);
          if (user) {
            const roomId = user.roomId;
            const usersInRoom = Array.from(whisperUsers.values()).filter(u => u.roomId === roomId);
            const broadcastMsg = JSON.stringify({
              type: 'room_message',
              from: clientId,
              fromName: user.name,
              roomId,
              content,
              timestamp: Date.now()
            });
            usersInRoom.forEach(u => {
              if (u.ws.readyState === WebSocket.OPEN) {
                u.ws.send(broadcastMsg);
              }
            });
          }
          break;
        }

        case 'private_file_chunk': {
          const { to, chunk, fileId, isLast, fileName, fileType, fileSize } = message;
          const targetUser = whisperUsers.get(to);
          if (targetUser && targetUser.ws.readyState === WebSocket.OPEN) {
            targetUser.ws.send(JSON.stringify({
              type: 'private_file_chunk',
              from: clientId,
              fromName: whisperUsers.get(clientId)?.name || 'Unknown',
              chunk,
              fileId,
              isLast,
              fileName,
              fileType,
              fileSize,
              timestamp: Date.now()
            }));
          }
          break;
        }
        case 'file_message': {
          const { to, fileId, fileName, fileSize, mimeType } = message;
          const targetUser = whisperUsers.get(to);
          if (targetUser && targetUser.ws.readyState === WebSocket.OPEN) {
            targetUser.ws.send(JSON.stringify({
              type: 'file_message',
              from: clientId,
              fromName: whisperUsers.get(clientId)?.name || 'Unknown',
              fileId,
              fileName,
              fileSize,
              mimeType,
              timestamp: Date.now()
            }));
          }
          break;
        }
        case 'typing': {
          const { to, isTyping } = message;
          const targetUser = whisperUsers.get(to);
          if (targetUser && targetUser.ws.readyState === WebSocket.OPEN) {
            targetUser.ws.send(JSON.stringify({
              type: 'typing',
              from: clientId,
              isTyping
            }));
          }
          break;
        }
        case 'leave': {
          if (whisperUsers.has(clientId)) {
            const user = whisperUsers.get(clientId);
            const roomId = user.roomId;
            console.log(`Whisper: User ID (${clientId}) left room ${roomId} explicitly.`);
            whisperUsers.delete(clientId);
            broadcastWhisperUserList(roomId);
          }
          break;
        }

      } // end switch
    } catch (err) {
      console.error('Whisper message error:', err);
    }
  });


  ws.on('close', () => {
    if (whisperUsers.has(clientId)) {
      const user = whisperUsers.get(clientId);
      const roomId = user.roomId;
      console.log(`Whisper: User ID (${clientId}) disconnected from room ${roomId}. Total: ${whisperUsers.size - 1}`);
      whisperUsers.delete(clientId);
      broadcastWhisperUserList(roomId);

      // Ephemeral cleanup: delete all files uploaded by this user
      for (const [fileId, fileData] of whisperFiles.entries()) {
        if (fileData.uploadedBy === clientId) {
          if (fs.existsSync(fileData.path)) fs.unlinkSync(fileData.path);
          whisperFiles.delete(fileId);
        }
      }
    }
  });

  ws.on('error', (err) => {
    console.error('Whisper WS error:', err.message);
    if (whisperUsers.has(clientId)) {
      const user = whisperUsers.get(clientId);
      const roomId = user.roomId;
      whisperUsers.delete(clientId);
      broadcastWhisperUserList(roomId);
      for (const [fileId, fileData] of whisperFiles.entries()) {
        if (fileData.uploadedBy === clientId) {
          if (fs.existsSync(fileData.path)) fs.unlinkSync(fileData.path);
          whisperFiles.delete(fileId);
        }
      }
    }
  });

});

function broadcastWhisperUserList(roomId) {
  const usersInRoom = Array.from(whisperUsers.values()).filter(u => u.roomId === roomId);
  const userList = usersInRoom.map(u => ({
    id: u.id,
    name: u.name,
    joinedAt: u.joinedAt
  }));

  const message = JSON.stringify({
    type: 'user_list',
    users: userList
  });

  usersInRoom.forEach(user => {
    if (user.ws.readyState === WebSocket.OPEN) {
      user.ws.send(message);
    }
  });
}

// Broadcast message to all clients in a session
function broadcastToSession(sessionId, message, excludeClientId) {
  wss.clients.forEach(client => {
    if (client.sessionId === sessionId && client.readyState === WebSocket.OPEN) {
      if (excludeClientId && client.clientId === excludeClientId) return;
      client.send(JSON.stringify(message));
    }
  });
}

// Track download statistics
function trackDownloadStart(sessionId, fileId, clientId) {
  const key = `${sessionId}-${fileId}`;
  if (!downloadStats.has(key)) {
    downloadStats.set(key, {
      sessionId,
      fileId,
      started: 0,
      completed: 0,
      failed: 0,
      active: 0
    });
  }

  const stats = downloadStats.get(key);
  stats.started++;
  stats.active++;
  downloadStats.set(key, stats);

  console.log(`Download started: ${key} (Total started: ${stats.started})`);
}

function trackDownloadComplete(sessionId, fileId) {
  const key = `${sessionId}-${fileId}`;
  if (downloadStats.has(key)) {
    const stats = downloadStats.get(key);
    stats.completed++;
    stats.active = Math.max(0, stats.active - 1);
    downloadStats.set(key, stats);

    console.log(`Download completed: ${key} (Completed: ${stats.completed})`);

    // Broadcast updated stats to sender
    broadcastToSession(sessionId, {
      type: 'download_stats',
      fileId,
      stats
    });
  }
}

function trackDownloadFailure(sessionId, fileId, error) {
  const key = `${sessionId}-${fileId}`;
  if (downloadStats.has(key)) {
    const stats = downloadStats.get(key);
    stats.failed++;
    stats.active = Math.max(0, stats.active - 1);
    downloadStats.set(key, stats);

    console.log(`Download failed: ${key} (Failed: ${stats.failed}, Error: ${error})`);

    // Broadcast failure to sender
    broadcastToSession(sessionId, {
      type: 'download_failed',
      fileId,
      error,
      stats
    });
  }
}

// Serve static files from the React app
// Improved path detection for Render/Docker vs Local
let frontendPath = path.join(__dirname, '../frontend/dist');

// Fallback for some hosting environments like Render where paths might be different
if (!fs.existsSync(frontendPath)) {
  const possiblePaths = [
    path.join(process.cwd(), 'frontend/dist'),
    path.join(__dirname, '../../frontend/dist'),
    '/opt/render/project/src/frontend/dist' // Render specific
  ];

  for (const p of possiblePaths) {
    if (fs.existsSync(p)) {
      frontendPath = p;
      break;
    }
  }
}

if (fs.existsSync(frontendPath)) {
  console.log(`Serving frontend from: ${frontendPath}`);
  app.use(express.static(frontendPath, {
    setHeaders: (res, filePath) => {
      // Set long-term cache for assets (JS, CSS, images) since they have hashes
      if (filePath.includes('assets') || filePath.match(/\.(js|css|woff2?|png|jpg|jpeg|gif|svg|ico|json)$/)) {
        res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
      } else if (filePath.endsWith('.html')) {
        // DO NOT CACHE index.html to ensure users get updates
        res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
        res.setHeader('Pragma', 'no-cache');
        res.setHeader('Expires', '0');
      }
    }
  }));

  // Specific 404 handler for API routes to prevent returning index.html
  app.all('/api/*', (req, res) => {
    res.status(404).json({ error: 'API route not found' });
  });

  // Handle SPA routing - serve index.html for all non-API routes
  app.get('*', (req, res, next) => {
    // Skip if it is an API request or static asset that was missed
    if (req.path.startsWith('/api/') || req.path.includes('.')) {
      return next();
    }
    res.sendFile(path.join(frontendPath, 'index.html'), (err) => {
      if (err) {
        // If index.html is missing, just send the backend status as fallback
        if (!res.headersSent) {
          res.json({ status: 'Heksta backend running 🚀', note: 'Frontend build not found' });
        }
      }
    });
  });
} else {
  console.warn(`WARNING: Frontend path not found at ${frontendPath}. Frontend will not be served.`);
  // Fallback root route if frontend is not available
  app.get('/', (req, res) => {
    res.json({ status: 'Heksta backend running 🚀', note: 'Static frontend not found' });
  });
}


// Global error handler for API routes
app.use('/api', (err, req, res, next) => {
  console.error('API Error:', err);
  if (!res.headersSent) {
    res.status(500).json({ error: 'Internal server error: ' + err.message });
  }
});

// Start server
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

// Cleanup inactive sessions
setInterval(() => {
  const now = Date.now();
  sessions.forEach((session, sessionId) => {
    if (now - session.createdAt > 4 * 60 * 60 * 1000) { // 4 hours
      sessions.delete(sessionId);
      const uploadDir = path.join(__dirname, 'uploads', sessionId);
      if (fs.existsSync(uploadDir)) {
        fs.rmSync(uploadDir, { recursive: true, force: true });
      }
    }
  });
}, 30 * 60 * 1000); // Check every 30 minutes