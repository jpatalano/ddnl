const express = require('express');
const path = require('path');
const app = express();
const PORT = process.env.PORT || 3000;

// Serve static assets with caching, but never cache HTML
app.use(express.static(path.join(__dirname), {
  etag: false,
  setHeaders: (res, filePath) => {
    if (filePath.endsWith('.html')) {
      res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
      res.setHeader('Pragma', 'no-cache');
    }
  }
}));
app.get('/debug', (req, res) => res.sendFile(path.join(__dirname, 'debug.html')));
app.get('*', (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
  res.sendFile(path.join(__dirname, 'index.html'));
});
app.listen(PORT, () => console.log(`FCC Analytics running on port ${PORT}`));
