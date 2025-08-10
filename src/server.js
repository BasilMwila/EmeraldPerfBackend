// server.js - Node.js API Service for Dashboard Data

// IMPORTANT: Load environment variables first!
require('dotenv').config();
const path = require('path');
require('dotenv').config({ path: path.resolve(__dirname, '..', '.env') });
const express = require('express');
const mysql = require('mysql2/promise');
const cors = require('cors');
const moment = require('moment');

const app = express();
const PORT = process.env.PORT || 5001;

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
    // Based on sample data, use actual field names and values
    // Don't calculate gross_recovered - use individual recovery fields
    
    return {
      // Date field (use load_date)
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
      
      // Lending metrics - use correct field mapping based on Aug 6 data
      lending_transactions: parseFloat(row.lending_txns) || 0, // Note: this might be decimal in sample data
      lending_txns: parseFloat(row.lending_txns) || 0,
      gross_lent: parseFloat(row.gross_lent) || 0, // Total amount including fees (288,920.22)
      principal_lent: (parseFloat(row.gross_lent) || 0) - (parseFloat(row.sfee_lent) || 0), // Calculate as gross - sfee
      service_fee_lent: parseFloat(row.sfee_lent) || 0, // Service fees (47,019.22)
      sfee_lent: parseFloat(row.sfee_lent) || 0,
      
      // Fees charged
      late_fees_charged: parseFloat(row.late_fees_charged) || 0,
      setup_fees_charged: parseFloat(row.setup_fees_charged) || 0,
      interest_fees_charged: parseFloat(row.interest_fees_charged) || 0,
      daily_fees_charged: parseFloat(row.daily_fees_charged) || 0,
      
      // Recovery metrics - use raw values, don't calculate gross_recovered
      recovery_transactions: parseFloat(row.recovery_txns) || 0,
      recovery_txns: parseFloat(row.recovery_txns) || 0,
      principal_recovered: parseFloat(row.principal_recovered) || 0,
      service_fee_recovered: parseFloat(row.sfee_recovered) || 0,
      sfee_recovered: parseFloat(row.sfee_recovered) || 0,
      
      // Fees recovered
      late_fees_recovered: parseFloat(row.late_fees_recovered) || 0,
      setup_fees_recovered: parseFloat(row.setup_fees_recovered) || 0,
      interest_fees_recovered: parseFloat(row.interest_fees_recovered) || 0,
      daily_fees_recovered: parseFloat(row.daily_fees_recovered) || 0,
      
      // Calculate gross_recovered as sum of individual recovery components
      gross_recovered: (parseFloat(row.principal_recovered) || 0) + 
                      (parseFloat(row.sfee_recovered) || 0) + 
                      (parseFloat(row.late_fees_recovered) || 0) + 
                      (parseFloat(row.setup_fees_recovered) || 0) + 
                      (parseFloat(row.interest_fees_recovered) || 0) + 
                      (parseFloat(row.daily_fees_recovered) || 0),
      
      // Additional fields for compatibility
      unique_users: parseInt(row.overall_actives_daily) || 0,
      overall_unique_users: parseInt(row.overall_actives_ytd) || 0,
      fx_rate: 1.0,
      
      // Metadata
      processed_at: row.processed_at ? moment(row.processed_at).format('YYYY-MM-DD HH:mm:ss') : '',
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
      days = '7', // Reduce default to 7 days for better performance
      start_date,
      end_date,
      limit = '1000' // Add limit parameter for pagination
    } = req.query;

    // Determine which tables to query - be more strict about telco filtering
    const tables = [];
    if (telco.toLowerCase() === 'airtel') {
      tables.push('airtel_loan_data');
      console.log('🔍 Filtering to AIRTEL ONLY for accurate comparison');
    } else if (telco.toLowerCase() === 'mtn') {
      tables.push('MTN_loan_data');
    } else if (telco.toLowerCase() === 'both') {
      tables.push('airtel_loan_data');
      tables.push('MTN_loan_data');
    }

    if (tables.length === 0) {
      return res.status(400).json({ error: 'Invalid telco parameter' });
    }

    // Build WHERE conditions
    const whereConditions = [];
    const params = [];

    // Date filtering - use load_date
    if (start_date && end_date) {
      whereConditions.push('load_date BETWEEN ? AND ?');
      params.push(start_date, end_date);
    } else {
      const daysInt = parseInt(days) || 7;
      whereConditions.push('load_date >= DATE_SUB(CURDATE(), INTERVAL ? DAY)');
      params.push(daysInt);
    }

    // Loan type filtering
    if (loan_type) {
      whereConditions.push('loan_type LIKE ?');
      params.push(`%${loan_type}%`);
    }
    
    // Additional telco filtering in SQL for extra safety
    if (telco.toLowerCase() === 'airtel') {
      whereConditions.push("(telco = 'Airtel' OR telco = 'airtel')");
      console.log('🔍 Adding SQL filter for Airtel telco only');
    } else if (telco.toLowerCase() === 'mtn') {
      whereConditions.push("(telco = 'MTN' OR telco = 'mtn')");
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
        FROM ${table} t1
        WHERE ${whereClause}
        AND t1.processed_at = (
          SELECT MAX(t2.processed_at) 
          FROM ${table} t2 
          WHERE t2.load_date = t1.load_date 
          AND t2.loan_type = t1.loan_type 
          AND t2.denom = t1.denom
        )
        ORDER BY load_date DESC, loan_type
        LIMIT ?
      `;

      const queryParams = [...params, parseInt(limit)];
      console.log('🔍 SQL Query:', query.replace(/\s+/g, ' ').trim());
      console.log('🔍 Query Params:', queryParams);
      console.log('🔍 Parameter types:', queryParams.map(p => typeof p));
      console.log('🔍 Parameter values:', queryParams.map(p => JSON.stringify(p)));
      const [rows] = await pool.query(query, queryParams);
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
    const { 
      telco = 'both', 
      days = '7', // Reduce default for better performance
      start_date, 
      end_date,
      limit = '500' // Add limit for specific loan types
    } = req.query;

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

    // Determine tables to query - be strict about telco filtering
    const tables = [];
    if (telco.toLowerCase() === 'airtel') {
      tables.push('airtel_loan_data');
      console.log('🔍 Specific loan type - filtering to AIRTEL ONLY');
    } else if (telco.toLowerCase() === 'mtn') {
      tables.push('MTN_loan_data');
    } else if (telco.toLowerCase() === 'both') {
      tables.push('airtel_loan_data');
      tables.push('MTN_loan_data');
    }

    let allResults = [];

    // Build WHERE conditions for date filtering
    const whereConditions = ['loan_type LIKE ?'];
    let params = [`%${dbLoanType}%`];

    // Date filtering - use load_date
    if (start_date && end_date) {
      whereConditions.push('load_date BETWEEN ? AND ?');
      params.push(start_date, end_date);
    } else {
      const daysInt = parseInt(days) || 7;
      whereConditions.push('load_date >= DATE_SUB(CURDATE(), INTERVAL ? DAY)');
      params.push(daysInt);
    }

    // Additional telco filtering for specific loan type endpoint
    if (telco.toLowerCase() === 'airtel') {
      whereConditions.push("(telco = 'Airtel' OR telco = 'airtel')");
    } else if (telco.toLowerCase() === 'mtn') {
      whereConditions.push("(telco = 'MTN' OR telco = 'mtn')");
    }
    
    const whereClause = whereConditions.join(' AND ');

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
        FROM ${table} t1
        WHERE ${whereClause}
        AND t1.processed_at = (
          SELECT MAX(t2.processed_at) 
          FROM ${table} t2 
          WHERE t2.load_date = t1.load_date 
          AND t2.loan_type = t1.loan_type 
          AND t2.denom = t1.denom
        )
        ORDER BY load_date DESC
        LIMIT ?
      `;

      const queryParams = [...params, parseInt(limit)];
      console.log('🔍 SQL Query:', query.replace(/\s+/g, ' ').trim());
      console.log('🔍 Query Params:', queryParams);
      console.log('🔍 Parameter types:', queryParams.map(p => typeof p));
      console.log('🔍 Parameter values:', queryParams.map(p => JSON.stringify(p)));
      const [rows] = await pool.query(query, queryParams);
      const formattedData = convertToDashboardFormat(rows);
      allResults = allResults.concat(formattedData);
    }

    // Sort by date
    allResults.sort((a, b) => new Date(b.date) - new Date(a.date));

    res.json({
      data: allResults,
      loan_type: dbLoanType,
      count: allResults.length,
      filters: {
        loan_type: dbLoanType,
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

      const [rows] = await pool.query(query, [parseInt(days)]);
      
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
    console.log('=== NPL DEBUG: Complete NPL query with JOIN ===');
    
    // Complete query with JOIN to get both outstanding and recovered data
    // Use latest available date for unrecovered table since it might be one day behind
    const query = `
      SELECT 
        o.loan_type,
        o.total_balance,
        o.within_tenure,
        o.arrears_30_days,
        o.arrears_31_60_days,
        o.arrears_61_90_days,
        o.arrears_91_120_days,
        o.arrears_121_150_days,
        o.arrears_151_180_days,
        o.arrears_181_plus_days,
        ROUND(((o.total_balance - o.within_tenure) / o.total_balance * 100), 2) as arrears_percentage,
        o.report_date,
        COALESCE(r.total_balance, 0) as net_recovered_value,
        COALESCE(u.total_balance, 
          ROUND(((o.total_balance) / (o.total_balance + COALESCE(r.total_balance, 0)) * 100), 2)
        ) as unrecovered_percentage_net
      FROM airtel_npl_outstanding_balance_net_summary o
      LEFT JOIN airtel_npl_net_recovered_value_summary r 
        ON o.loan_type = r.loan_type 
        AND DATE(o.report_date) = DATE(r.report_date)
      LEFT JOIN airtel_npl_unrecovered_percentage_summary u 
        ON o.loan_type = u.loan_type 
        AND DATE(u.report_date) = (SELECT MAX(DATE(report_date)) FROM airtel_npl_unrecovered_percentage_summary WHERE loan_type = o.loan_type)
      WHERE o.report_date = (SELECT MAX(report_date) FROM airtel_npl_outstanding_balance_net_summary)
      ORDER BY 
        CASE o.loan_type
          WHEN '7 Days Loan' THEN 1
          WHEN '14 Days Loan' THEN 2
          WHEN '21 Days Loan' THEN 3
          WHEN '30 Days Loan' THEN 4
          WHEN 'Grand Total' THEN 5
          ELSE 6
        END
    `;
    
    console.log('NPL Query:', query);

    const [rows] = await pool.query(query);
    
    console.log('=== NPL DEBUG: JOIN Results ===');
    rows.forEach(row => {
      console.log(`${row.loan_type}:`);
      console.log(`  Total Balance: ${row.total_balance}`);
      console.log(`  Net Recovered: ${row.net_recovered_value}`);
      console.log(`  Unrecovered %: ${row.unrecovered_percentage_net}`);
      console.log(`  All arrears fields present: ${!!(row.arrears_31_60_days && row.arrears_61_90_days && row.arrears_91_120_days)}`);
    });
    
    const nplData = rows.map(row => ({
      ...row,
      report_date: moment(row.report_date).format('YYYY-MM-DD'),
      // Use direct field mapping from database
      total_balance: parseFloat(row.total_balance) || 0,
      within_tenure: parseFloat(row.within_tenure) || 0,
      arrears_30_days: parseFloat(row.arrears_30_days) || 0,
      arrears_31_60_days: parseFloat(row.arrears_31_60_days) || 0,
      arrears_61_90_days: parseFloat(row.arrears_61_90_days) || 0,
      arrears_91_120_days: parseFloat(row.arrears_91_120_days) || 0,
      arrears_121_150_days: parseFloat(row.arrears_121_150_days) || 0,
      arrears_151_180_days: parseFloat(row.arrears_151_180_days) || 0,
      arrears_181_plus_days: parseFloat(row.arrears_181_plus_days) || 0,
      // Recovered and percentage data from JOINed tables
      net_recovered_value: parseFloat(row.net_recovered_value) || 0,
      unrecovered_percentage_net: parseFloat(row.unrecovered_percentage_net) || 0,
      // Calculated percentages
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

    const [airtelResult] = await pool.query(airtelQuery);
    const [mtnResult] = await pool.query(mtnQuery);

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

// Test endpoint to check table structure
app.get('/api/test-table-structure', async (req, res) => {
  try {
    const [rows] = await pool.query('DESCRIBE airtel_loan_data');
    res.json({ columns: rows });
  } catch (error) {
    console.error('Table structure check failed:', error);
    res.status(500).json({ error: 'Table structure check failed', details: error.message });
  }
});

// Test endpoint with simple query
app.get('/api/test-simple-query', async (req, res) => {
  try {
    const query = `SELECT load_date, loan_type, denom, gross_lent FROM airtel_loan_data LIMIT 10`;
    console.log('🔍 Simple Test Query:', query);
    const [rows] = await pool.query(query);
    res.json({ data: rows, count: rows.length });
  } catch (error) {
    console.error('Simple query failed:', error);
    res.status(500).json({ error: 'Simple query failed', details: error.message });
  }
});

// Test NPL table structures and date ranges
app.get('/api/test-npl-tables', async (req, res) => {
  try {
    const results = {};
    
    // Test airtel_npl_outstanding_balance_net_summary
    const query1 = `SELECT * FROM airtel_npl_outstanding_balance_net_summary LIMIT 1`;
    console.log('🔍 Testing outstanding table:', query1);
    const [rows1] = await pool.query(query1);
    results.outstanding_table = { columns: Object.keys(rows1[0] || {}), sample_data: rows1[0] };
    
    // Test airtel_npl_net_recovered_value_summary
    const query2 = `SELECT * FROM airtel_npl_net_recovered_value_summary LIMIT 1`;
    console.log('🔍 Testing recovered table:', query2);
    const [rows2] = await pool.query(query2);
    results.recovered_table = { columns: Object.keys(rows2[0] || {}), sample_data: rows2[0] };
    
    // Test airtel_npl_unrecovered_percentage_summary
    const query3 = `SELECT * FROM airtel_npl_unrecovered_percentage_summary LIMIT 1`;
    console.log('🔍 Testing unrecovered table:', query3);
    const [rows3] = await pool.query(query3);
    results.unrecovered_table = { columns: Object.keys(rows3[0] || {}), sample_data: rows3[0] };
    
    // Check available dates in all tables
    const dateQuery1 = `SELECT DISTINCT DATE(report_date) as date FROM airtel_npl_outstanding_balance_net_summary ORDER BY date DESC LIMIT 5`;
    const [datRows1] = await pool.query(dateQuery1);
    results.outstanding_dates = datRows1;
    
    const dateQuery2 = `SELECT DISTINCT DATE(report_date) as date FROM airtel_npl_net_recovered_value_summary ORDER BY date DESC LIMIT 5`;
    const [datRows2] = await pool.query(dateQuery2);
    results.recovered_dates = datRows2;
    
    const dateQuery3 = `SELECT DISTINCT DATE(report_date) as date FROM airtel_npl_unrecovered_percentage_summary ORDER BY date DESC LIMIT 5`;
    const [datRows3] = await pool.query(dateQuery3);
    results.unrecovered_dates = datRows3;
    
    res.json(results);
  } catch (error) {
    console.error('NPL table test failed:', error);
    res.status(500).json({ error: 'NPL table test failed', details: error.message });
  }
});

// Test what the actual API returns for Aug 6
app.get('/api/test-api-output', async (req, res) => {
  try {
    // Simulate the actual API call for Aug 6
    const apiUrl = `http://localhost:${process.env.PORT || 5000}/api/loan-data?start_date=2025-08-06&end_date=2025-08-06&telco=airtel`;
    console.log('🔍 Testing API URL:', apiUrl);
    
    // Make internal API call
    const fetch = require('node-fetch');
    const response = await fetch(apiUrl);
    const data = await response.json();
    
    // Calculate totals from API response
    const apiTotals = {
      record_count: data.data?.length || 0,
      total_lending_txns: data.data?.reduce((sum, row) => sum + (parseFloat(row.lending_txns) || 0), 0) || 0,
      total_gross_lent: data.data?.reduce((sum, row) => sum + (parseFloat(row.gross_lent) || 0), 0) || 0,
      total_sfee_lent: data.data?.reduce((sum, row) => sum + (parseFloat(row.sfee_lent) || 0), 0) || 0
    };
    
    res.json({
      api_response: data,
      calculated_totals: apiTotals
    });
  } catch (error) {
    console.error('API test failed:', error);
    res.status(500).json({ error: 'API test failed', details: error.message });
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