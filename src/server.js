// server.js - Node.js API Service for Dashboard Data

// IMPORTANT: Load environment variables first!
require('dotenv').config();

const express = require('express');
const mysql = require('mysql2/promise');
const cors = require('cors');
const moment = require('moment');

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors());
app.use(express.json());

// Database configuration - now will properly read from .env file
const dbConfig = {
  host: process.env.DB_HOST || 'your-default-host',
  user: process.env.DB_USER || 'dbmasteruser',
  password: process.env.DB_PASSWORD || 'your-password',
  database: process.env.DB_DATABASE || 'EmeraldFinanceLtd_db1',
  port: process.env.DB_PORT || 3306,
  charset: 'utf8mb4'
};

// Debug: Log database config (remove password for security)
console.log('🔧 Database Config:', {
  host: dbConfig.host,
  user: dbConfig.user,
  database: dbConfig.database,
  port: dbConfig.port
});

// Create database connection pool
const pool = mysql.createPool({
  ...dbConfig,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

// Test database connection on startup
async function testDatabaseConnection() {
  try {
    const connection = await pool.getConnection();
    console.log('✅ Database connected successfully');
    connection.release();
  } catch (error) {
    console.error('❌ Database connection failed:', error.message);
    process.exit(1);
  }
}

// Utility function to convert database rows to dashboard format
const convertToDashboardFormat = (rows) => {
  return rows.map(row => {
    // Calculate derived fields
    const grossRecovered = (parseFloat(row.principal_recovered) || 0) + (parseFloat(row.sfee_recovered) || 0);
    const principalLent = (parseFloat(row.gross_lent) || 0) - (parseFloat(row.sfee_lent) || 0);
    
    return {
      // Date field (load_date -> date)
      date: moment(row.load_date).format('YYYY-MM-DD'),
      
      // Basic info
      telco: row.telco || '',
      country: row.country || '',
      loan_type: row.loan_type || '',
      denom: parseInt(row.denom) || 0,
      
      // Base metrics
      qualified_base: parseInt(row.qualified_base) || 0,
      overall_actives_daily: parseInt(row.overall_actives_daily) || 0,
      overall_actives_wtd: parseFloat(row.overall_actives_wtd) || 0,
      overall_actives_mtd: parseInt(row.overall_actives_mtd) || 0,
      overall_actives_ytd: parseInt(row.overall_actives_ytd) || 0,
      
      // Lending metrics
      lending_transactions: parseInt(row.lending_txns) || 0,
      lending_txns: parseInt(row.lending_txns) || 0,
      gross_lent: parseFloat(row.gross_lent) || 0,
      principal_lent: principalLent,
      service_fee_lent: parseFloat(row.sfee_lent) || 0,
      sfee_lent: parseFloat(row.sfee_lent) || 0,
      
      // Fees charged
      late_fees_charged: parseFloat(row.late_fees_charged) || 0,
      setup_fees_charged: parseFloat(row.setup_fees_charged) || 0,
      interest_fees_charged: parseFloat(row.interest_fees_charged) || 0,
      daily_fees_charged: parseFloat(row.daily_fees_charged) || 0,
      
      // Recovery metrics
      recovery_transactions: parseInt(row.recovery_txns) || 0,
      recovery_txns: parseInt(row.recovery_txns) || 0,
      gross_recovered: grossRecovered,
      principal_recovered: parseFloat(row.principal_recovered) || 0,
      service_fee_recovered: parseFloat(row.sfee_recovered) || 0,
      sfee_recovered: parseFloat(row.sfee_recovered) || 0,
      
      // Fees recovered
      late_fees_recovered: parseFloat(row.late_fees_recovered) || 0,
      setup_fees_recovered: parseFloat(row.setup_fees_recovered) || 0,
      interest_fees_recovered: parseFloat(row.interest_fees_recovered) || 0,
      daily_fees_recovered: parseFloat(row.daily_fees_recovered) || 0,
      
      // Additional fields for compatibility
      unique_users: parseInt(row.overall_actives_daily) || 0,
      overall_unique_users: parseInt(row.overall_actives_ytd) || 0,
      fx_rate: 1.0,
      
      // Metadata
      processed_at: moment(row.processed_at).format('YYYY-MM-DD HH:mm:ss'),
      file_source: row.file_source || ''
    };
  });
};

// Health check endpoint
app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    timestamp: new Date().toISOString(),
    database: 'connected'
  });
});

// Get loan data with filters
app.get('/api/loan-data', async (req, res) => {
  try {
    const {
      loan_type,
      telco = 'both',
      days = '30',
      start_date,
      end_date
    } = req.query;

    // Determine which tables to query
    const tables = [];
    if (telco.toLowerCase() === 'airtel' || telco.toLowerCase() === 'both') {
      tables.push('airtel_loan_data');
    }
    if (telco.toLowerCase() === 'mtn' || telco.toLowerCase() === 'both') {
      tables.push('MTN_loan_data');
    }

    if (tables.length === 0) {
      return res.status(400).json({ error: 'Invalid telco parameter' });
    }

    // Build WHERE conditions
    const whereConditions = [];
    const params = [];

    // Date filtering
    if (start_date && end_date) {
      whereConditions.push('load_date BETWEEN ? AND ?');
      params.push(start_date, end_date);
    } else {
      whereConditions.push('load_date >= DATE_SUB(CURDATE(), INTERVAL ? DAY)');
      params.push(parseInt(days));
    }

    // Loan type filtering
    if (loan_type) {
      whereConditions.push('loan_type LIKE ?');
      params.push(`%${loan_type}%`);
    }

    const whereClause = whereConditions.length > 0 ? whereConditions.join(' AND ') : '1=1';

    // Execute queries for each table
    let allResults = [];

    for (const table of tables) {
      const query = `
        SELECT 
          load_date, loan_type, denom, gross_lent, sfee_lent, lending_txns,
          late_fees_charged, principal_recovered, sfee_recovered, 
          late_fees_recovered, recovery_txns, interest_fees_charged,
          interest_fees_recovered, setup_fees_charged, setup_fees_recovered,
          daily_fees_charged, daily_fees_recovered, country, telco,
          qualified_base, overall_actives_daily, overall_actives_wtd,
          overall_actives_mtd, overall_actives_ytd, processed_at, file_source
        FROM ${table}
        WHERE ${whereClause}
        ORDER BY load_date DESC, loan_type
      `;

      const [rows] = await pool.execute(query, params);
      const formattedData = convertToDashboardFormat(rows);
      allResults = allResults.concat(formattedData);
    }

    // Sort combined results by date
    allResults.sort((a, b) => new Date(b.date) - new Date(a.date));

    res.json({
      data: allResults,
      count: allResults.length,
      filters: {
        loan_type,
        telco,
        days: parseInt(days),
        start_date,
        end_date
      }
    });

  } catch (error) {
    console.error('Database query failed:', error);
    res.status(500).json({ error: 'Database query failed', details: error.message });
  }
});

// Get loan data by specific loan type (for individual dashboard pages)
app.get('/api/loan-data/:loanType', async (req, res) => {
  try {
    const { loanType } = req.params;
    const { telco = 'both', days = '30' } = req.query;

    // Map loan type to database format
    const loanTypeMap = {
      '7': 'Nano 7D',
      '14': 'Nano 14D', 
      '21': 'Nano 21D',
      '30': 'Nano 30D'
    };

    const dbLoanType = loanTypeMap[loanType];
    if (!dbLoanType) {
      return res.status(400).json({ error: 'Invalid loan type' });
    }

    // Determine tables to query
    const tables = [];
    if (telco.toLowerCase() === 'airtel' || telco.toLowerCase() === 'both') {
      tables.push('airtel_loan_data');
    }
    if (telco.toLowerCase() === 'mtn' || telco.toLowerCase() === 'both') {
      tables.push('MTN_loan_data');
    }

    let allResults = [];

    for (const table of tables) {
      const query = `
        SELECT 
          load_date, loan_type, denom, gross_lent, sfee_lent, lending_txns,
          late_fees_charged, principal_recovered, sfee_recovered, 
          late_fees_recovered, recovery_txns, interest_fees_charged,
          interest_fees_recovered, setup_fees_charged, setup_fees_recovered,
          daily_fees_charged, daily_fees_recovered, country, telco,
          qualified_base, overall_actives_daily, overall_actives_wtd,
          overall_actives_mtd, overall_actives_ytd, processed_at, file_source
        FROM ${table}
        WHERE loan_type LIKE ? 
          AND load_date >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
        ORDER BY load_date DESC
      `;

      const [rows] = await pool.execute(query, [`%${dbLoanType}%`, parseInt(days)]);
      const formattedData = convertToDashboardFormat(rows);
      allResults = allResults.concat(formattedData);
    }

    // Sort by date
    allResults.sort((a, b) => new Date(b.date) - new Date(a.date));

    res.json({
      data: allResults,
      loan_type: dbLoanType,
      count: allResults.length
    });

  } catch (error) {
    console.error('Database query failed:', error);
    res.status(500).json({ error: 'Database query failed', details: error.message });
  }
});

// Get aggregated loan data summary
app.get('/api/loan-data/summary', async (req, res) => {
  try {
    const { telco = 'both', days = '30' } = req.query;

    const tables = [];
    if (telco.toLowerCase() === 'airtel' || telco.toLowerCase() === 'both') {
      tables.push('airtel_loan_data');
    }
    if (telco.toLowerCase() === 'mtn' || telco.toLowerCase() === 'both') {
      tables.push('MTN_loan_data');
    }

    let allSummaries = [];

    for (const table of tables) {
      const query = `
        SELECT 
          loan_type,
          COUNT(*) as record_count,
          SUM(gross_lent) as total_gross_lent,
          SUM(principal_recovered) as total_principal_recovered,
          SUM(sfee_recovered) as total_service_fee_recovered,
          SUM(late_fees_recovered) as total_late_fees_recovered,
          SUM(lending_txns) as total_lending_transactions,
          SUM(recovery_txns) as total_recovery_transactions,
          AVG(qualified_base) as avg_qualified_base,
          MAX(load_date) as latest_date,
          telco
        FROM ${table}
        WHERE load_date >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
        GROUP BY loan_type, telco
        ORDER BY loan_type, telco
      `;

      const [rows] = await pool.execute(query, [parseInt(days)]);
      
      const formattedSummaries = rows.map(row => ({
        ...row,
        latest_date: moment(row.latest_date).format('YYYY-MM-DD'),
        total_gross_lent: parseFloat(row.total_gross_lent) || 0,
        total_principal_recovered: parseFloat(row.total_principal_recovered) || 0,
        total_service_fee_recovered: parseFloat(row.total_service_fee_recovered) || 0,
        total_late_fees_recovered: parseFloat(row.total_late_fees_recovered) || 0,
        avg_qualified_base: parseFloat(row.avg_qualified_base) || 0
      }));

      allSummaries = allSummaries.concat(formattedSummaries);
    }

    res.json({
      summary: allSummaries,
      filters: { telco, days: parseInt(days) }
    });

  } catch (error) {
    console.error('Summary query failed:', error);
    res.status(500).json({ error: 'Summary query failed', details: error.message });
  }
});

// Get NPL data
app.get('/api/npl-data', async (req, res) => {
  try {
    const query = `
      SELECT 
        loan_type,
        total_balance,
        within_tenure,
        arrears_30_days,
        arrears_181_plus_days,
        ROUND(((total_balance - within_tenure) / total_balance * 100), 2) as arrears_percentage,
        report_date
      FROM airtel_npl_outstanding_balance_net_summary 
      WHERE report_date = (SELECT MAX(report_date) FROM airtel_npl_outstanding_balance_net_summary)
      ORDER BY 
        CASE loan_type
          WHEN '7 Days Loan' THEN 1
          WHEN '14 Days Loan' THEN 2
          WHEN '21 Days Loan' THEN 3
          WHEN '30 Days Loan' THEN 4
          WHEN 'Grand Total' THEN 5
          ELSE 6
        END
    `;

    const [rows] = await pool.execute(query);
    
    const nplData = rows.map(row => ({
      ...row,
      report_date: moment(row.report_date).format('YYYY-MM-DD'),
      total_balance: parseFloat(row.total_balance) || 0,
      within_tenure: parseFloat(row.within_tenure) || 0,
      arrears_30_days: parseFloat(row.arrears_30_days) || 0,
      arrears_181_plus_days: parseFloat(row.arrears_181_plus_days) || 0,
      arrears_percentage: parseFloat(row.arrears_percentage) || 0
    }));

    res.json({
      npl_data: nplData,
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    console.error('NPL query failed:', error);
    res.status(500).json({ error: 'NPL query failed', details: error.message });
  }
});

// Get latest data processing status
app.get('/api/status', async (req, res) => {
  try {
    // Check latest data in both tables
    const airtelQuery = `
      SELECT 
        MAX(load_date) as latest_date,
        COUNT(*) as total_records,
        'Airtel' as telco
      FROM airtel_loan_data
    `;
    
    const mtnQuery = `
      SELECT 
        MAX(load_date) as latest_date,
        COUNT(*) as total_records,
        'MTN' as telco
      FROM MTN_loan_data
    `;

    const [airtelResult] = await pool.execute(airtelQuery);
    const [mtnResult] = await pool.execute(mtnQuery);

    res.json({
      status: 'active',
      data_status: {
        airtel: {
          latest_date: airtelResult[0].latest_date ? moment(airtelResult[0].latest_date).format('YYYY-MM-DD') : null,
          total_records: airtelResult[0].total_records
        },
        mtn: {
          latest_date: mtnResult[0].latest_date ? moment(mtnResult[0].latest_date).format('YYYY-MM-DD') : null,
          total_records: mtnResult[0].total_records
        }
      },
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    console.error('Status query failed:', error);
    res.status(500).json({ error: 'Status query failed', details: error.message });
  }
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

// Initialize server
async function startServer() {
  try {
    // Test database connection first
    await testDatabaseConnection();
    
    // Start server
    app.listen(PORT, () => {
      console.log(`🚀 API Server running on port ${PORT}`);
      console.log(`📊 Dashboard API endpoints:`);
      console.log(`   GET /api/health - Health check`);
      console.log(`   GET /api/loan-data - Get loan data with filters`);
      console.log(`   GET /api/loan-data/:loanType - Get specific loan type data`);
      console.log(`   GET /api/loan-data/summary - Get aggregated summary`);
      console.log(`   GET /api/npl-data - Get NPL data`);
      console.log(`   GET /api/status - Get data processing status`);
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received, shutting down gracefully');
  await pool.end();
  process.exit(0);
});

// Start the server
startServer();

module.exports = app;