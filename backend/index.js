const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const sql = require('mssql');
require('dotenv').config();


const config = {
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    server: process.env.DB_SERVER,
    port: parseInt(process.env.DB_PORT),
    database: process.env.DB_NAME,
    options: {
        encrypt: process.env.DB_ENCRYPT === 'true',
        trustServerCertificate: process.env.DB_TRUST_CERT === 'true'
    },
};

// ✅ Sanitize column names
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

// ✅ Create table (with dedicated pool)
async function createTable(pool, tableName, columns) {
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
    await pool.request().query(createTableSql);
    console.log(`✅ Table "${tableName}" is ready.`);
}

// ✅ Insert a batch
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
    console.log(`✅ Inserted ${batch.length} rows into "${tableName}"`);
}

// ✅ Log upload metadata
async function logUploadHistory(pool, tableName, fileName, rowCount, email) {
    const query = `
        INSERT INTO UploadHistory (filename, tableName, rowCoun, emailid)
        VALUES (@filename, @tableName, @rowCoun, @emailid);
    `;
    await pool.request()
        .input('filename', sql.NVarChar, fileName)
        .input('tableName', sql.NVarChar, tableName)
        .input('rowCoun', sql.Int, rowCount)
        .input('emailid', sql.NVarChar, email)
        .query(query);
    console.log('📊 Upload history logged.');
}

//  Main function
async function importCsvStream(filePath, tableName, email) {
    return new Promise((resolve, reject) => {
        let originalColumns = [];
        let sanitizedColumns = [];
        let pool = null;
        let buffer = [];
        let maxRowsPerBatch = 500;
        let totalRows = 0;

        const stream = fs.createReadStream(filePath).pipe(csv());
        stream.pause();

        let inserting = false;
        let endCalled = false;

        async function flushBuffer() {
            if (buffer.length === 0 || !pool) return;

            inserting = true;
            try {
                await insertBatch(pool, tableName, sanitizedColumns, buffer, originalColumns);
                totalRows += buffer.length;
                buffer = [];
            } catch (err) {
                if (pool) await pool.close();
                return reject(err);
            }
            inserting = false;

            if (endCalled) await finalize();
        }

    
        async function finalize() {
            try {
                // Log upload metadata
                await logUploadHistory(pool, tableName, path.basename(filePath), totalRows, email);

                // Close the dedicated pool
                if (pool) await pool.close();

                // Log a success message
                console.log('✅ CSV import complete.');

                // Resolve the promise
                resolve();
            } catch (err) {
                // If there's an error, close the pool and reject the promise
                if (pool) await pool.close();
                reject(err);
            }
        }

        stream.on('headers', async (headers) => {
            try {
                originalColumns = headers;
                sanitizedColumns = sanitizeColumnNames(headers);
                const columnCount = sanitizedColumns.length || 1;
                maxRowsPerBatch = Math.floor(2100 / columnCount);
                console.log(`🔢 Columns: ${columnCount}, Max rows/batch: ${maxRowsPerBatch}`);

                pool = new sql.ConnectionPool(config);
                await pool.connect();
                await createTable(pool, tableName, sanitizedColumns);

                stream.resume();
            } catch (err) {
                if (pool) await pool.close();
                reject(err);
            }
        });

        stream.on('data', (row) => {
            buffer.push(row);
            if (buffer.length >= maxRowsPerBatch && !inserting) {
                stream.pause();
                setImmediate(async () => {
                    await flushBuffer();
                    stream.resume();
                });
            }
        });

        stream.on('end', async () => {
            if (inserting) {
                endCalled = true;
            } else {
                await flushBuffer();
                await finalize();
            }
        });

        stream.on('error', async (err) => {
            if (pool) await pool.close();
            reject(err);
        });
    });
}

module.exports = {
    importCsvStream,
};
