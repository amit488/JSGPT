const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const sql = require('mssql');

const config = {
    user: 'sa',
    password: 'Password@',
    server: 'localhost',
    port: 1433,
    database: 'test_db',
    options: {
        encrypt: false,
        trustServerCertificate: true,
    },
};

// Sanitize column names for SQL Server
function sanitizeColumnNames(columns) {
    const seen = new Set();
    return columns.map((col, idx) => {
        let sanitized = col.trim() || `col_${idx + 1}`;
        sanitized = sanitized.replace(/[^a-zA-Z0-9_]/g, '_');
        if (/^\d/.test(sanitized)) sanitized = 'col_' + sanitized;
        let base = sanitized, count = 1;
        while (seen.has(sanitized)) sanitized = `${base}_${count++}`;
        seen.add(sanitized);
        return sanitized;
    });
}

// Create table if not exists
async function createTable(tableName, columns) {
    const columnsSql = columns.map(col => `[${col}] NVARCHAR(255) NULL`).join(', ');
    const createTableSql = `
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '${tableName}'
        )
        BEGIN
            CREATE TABLE [${tableName}] (
                id INT IDENTITY(1,1) PRIMARY KEY,
                ${columnsSql}
            );
        END
    `;
    const pool = await sql.connect(config);
    await pool.request().query(createTableSql);
    console.log(`Table "${tableName}" is ready.`);
    return pool;
}

// Insert a batch of rows
async function insertBatch(pool, tableName, columns, batch, originalColumns) {
    const request = pool.request();
    const valueRows = [];

    batch.forEach((row, i) => {
        const values = [];
        columns.forEach((col, j) => {
            const originalCol = originalColumns[j];
            const paramName = `p_${i}_${j}`;
            request.input(paramName, sql.NVarChar(255), row[originalCol] || null);
            values.push(`@${paramName}`);
        });
        valueRows.push(`(${values.join(', ')})`);
    });

    const columnsSql = columns.map(col => `[${col}]`).join(', ');
    const insertSql = `INSERT INTO [${tableName}] (${columnsSql}) VALUES ${valueRows.join(', ')}`;
    await request.query(insertSql);
}

// Log upload history metadata
async function logUploadHistory(pool, tableName, fileName, rowCount) {
    const query = `
        INSERT INTO UploadHistory (filename, tableName, rowCoun)
        VALUES (@filename, @tableName, @rowCoun);
    `;
    await pool.request()
        .input('filename', sql.NVarChar, fileName)
        .input('tableName', sql.NVarChar, tableName)
        .input('rowCoun', sql.Int, rowCount)
        .query(query);
    console.log('Upload history logged.');
}

// Main function to import CSV and log metadata
async function importCsvStream(filePath, tableName) {
    return new Promise((resolve, reject) => {
        let originalColumns = [];
        let sanitizedColumns = [];
        let pool;
        let buffer = [];
        let maxRowsPerBatch = 500;
        let totalRows = 0;

        const stream = fs.createReadStream(filePath).pipe(csv());
        stream.pause();

        stream.on('headers', async (headers) => {
            originalColumns = headers;
            sanitizedColumns = sanitizeColumnNames(headers);

            const columnCount = sanitizedColumns.length;
            maxRowsPerBatch = Math.floor(2100 / columnCount);

            try {
                pool = await createTable(tableName, sanitizedColumns);
                stream.resume();
            } catch (err) {
                reject(err);
            }
        });

        stream.on('data', async (row) => {
            stream.pause();
            buffer.push(row);

            if (buffer.length >= maxRowsPerBatch) {
                try {
                    await insertBatch(pool, tableName, sanitizedColumns, buffer, originalColumns);
                    totalRows += buffer.length;
                    buffer = [];
                } catch (err) {
                    reject(err);
                    return;
                }
            }

            stream.resume();
        });

        stream.on('end', async () => {
            try {
                if (buffer.length > 0) {
                    await insertBatch(pool, tableName, sanitizedColumns, buffer, originalColumns);
                    totalRows += buffer.length;
                }

                // Log to uploadhistory after import
                await logUploadHistory(pool, tableName, path.basename(filePath),totalRows);

                if (pool) await pool.close();
                console.log('CSV import complete.');
                resolve();
            } catch (err) {
                reject(err);
            }
        });

        stream.on('error', err => reject(err));
    });
}

module.exports = {
    importCsvStream,
};
