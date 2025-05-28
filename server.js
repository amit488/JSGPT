const express = require('express');
const multer = require('multer');
const path = require('path');
const cors = require('cors');
const nodemailer = require('nodemailer');
const { importCsvStream } = require('./backend/index.js');

const app = express();
const PORT = 3000;

// Middleware
app.use(cors({
  origin: process.env.CORS_ORIGIN,
}));
app.use(express.static(path.join(__dirname, 'frontend')));

// File Upload Setup
const storage = multer.diskStorage({
  destination: './backend/uploads',
  filename: (req, file, cb) => cb(null, file.originalname),
});
const upload = multer({ storage });

// Email Configuration
const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: process.env.EMAIL_USER,         // Replace with your email
    pass: process.env.EMAIL_PASS,                  // Use Gmail App Password (not your Gmail login password)
  },
});


function sendEmailNotification(filename) {
  const mailOptions = {
    from: process.env.EMAIL_FROM,
    to: process.env.EMAIL_TO, // Change to desired recipient
    subject: 'CSV Upload Successful',
    text: `The file '${filename}' was uploaded and processed successfully.`,
  };

  transporter.sendMail(mailOptions, (err, info) => {
    if (err) {
      return console.error('Failed to send email:', err);
    }
    console.log('Email sent:', info.response);
  });
}

// Upload Endpoint
app.post('/upload', upload.array('csvFile', 10), async (req, res) => {
  console.log('Upload endpoint hit');
  console.log(req.files);  // note: files, not file

  if (!req.files || req.files.length === 0) {
    return res.status(400).send('No files uploaded');
  }

  try {
    // Process files sequentially or in parallel
    for (const file of req.files) {
      const filePath = file.path;
      const fileName = file.originalname;
      const tableName = path.parse(fileName).name.replace(/[^a-zA-Z0-9_]/g, '_');
      const email = req.body.email || ''; // You might want to support email per request, or modify accordingly

      await importCsvStream(filePath, tableName, email);
      sendEmailNotification(fileName);
    }
    res.status(200).send('All CSV files uploaded and inserted successfully!');
  } catch (err) {
    console.error('Import error:', err);
    res.status(500).send('Failed to upload or insert CSV files');
  }
});

// Start Server
app.listen(PORT, () => console.log(`Server running at http://localhost:${PORT}`));
