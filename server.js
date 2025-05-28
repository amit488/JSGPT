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
  origin: 'http://localhost:3000',
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
    user: 'amitpanda77777@gmail.com',         // Replace with your email
    pass: 'oehoxhapipobmiwf',                  // Use Gmail App Password (not your Gmail login password)
  },
});

function sendEmailNotification(filename) {
  const mailOptions = {
    from: 'amitpanda77777@gmail.com',
    to: 'amitpanda77777@gmail.com', // Change to desired recipient
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
app.post('/upload', upload.single('csvFile'), async (req, res) => {
  console.log('Upload endpoint hit');
  console.log(req.file);

  if (!req.file) {
    return res.status(400).send('No file uploaded');
  }

  const filePath = req.file.path;
  const fileName = req.file.originalname;
  const tableName = path.parse(fileName).name.replace(/[^a-zA-Z0-9_]/g, '_');

  try {
    await importCsvStream(filePath, tableName);
    sendEmailNotification(fileName);  // Email on successful import
    res.status(200).send('CSV uploaded and inserted successfully!');
  } catch (err) {
    console.error('Import error:', err);
    res.status(500).send('Failed to upload or insert CSV');
  }
});

// Start Server
app.listen(PORT, () => console.log(`Server running at http://localhost:${PORT}`));
