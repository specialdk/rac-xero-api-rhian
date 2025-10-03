// RAC Financial Dashboard - Complete Server.js with Date Picker Support
// Includes all functionality: Database Token Storage, Trial Balance with Date Support, Multi-entity Integration
// RAC Financial Dashboard - Complete Server.js with Date Picker Support
// Includes all functionality: Database Token Storage, Trial Balance with Date Support, Multi-entity Integration

import dotenv from "dotenv";
import express from "express";
import path from "path";
import fetch from "node-fetch";
import pkg from "xero-node";
const { XeroAccessToken, XeroIdToken, XeroClient } = pkg;
import { Pool } from "pg";
import { fileURLToPath } from "url";
import { dirname } from "path";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = express();
const port = process.env.PORT || 3000;

// Environment variables
const XERO_CLIENT_ID = process.env.XERO_CLIENT_ID;
const XERO_CLIENT_SECRET = process.env.XERO_CLIENT_SECRET;
const XERO_REDIRECT_URI = process.env.XERO_REDIRECT_URI;

const APPROVALMAX_CLIENT_ID = process.env.APPROVALMAX_CLIENT_ID;
const APPROVALMAX_CLIENT_SECRET = process.env.APPROVALMAX_CLIENT_SECRET;
const APPROVALMAX_REDIRECT_URI = process.env.APPROVALMAX_REDIRECT_URI;

// PostgreSQL connection (Railway provides DATABASE_URL automatically)
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl:
    process.env.NODE_ENV === "production"
      ? { rejectUnauthorized: false }
      : false,
});

// ApprovalMax configuration
const APPROVALMAX_CONFIG = {
  authUrl: "https://identity.approvalmax.com/connect/authorize",
  tokenUrl: "https://identity.approvalmax.com/connect/token",
  apiUrl: "https://public-api.approvalmax.com/api/v1",
  scopes: [
    "https://www.approvalmax.com/scopes/public_api/read",
    "https://www.approvalmax.com/scopes/public_api/write",
    "offline_access",
  ],
};

// Initialize database tables
async function initializeDatabase() {
  try {
    // Create tokens table if it doesn't exist
    await pool.query(`
            CREATE TABLE IF NOT EXISTS tokens (
                id SERIAL PRIMARY KEY,
                tenant_id VARCHAR(255) UNIQUE NOT NULL,
                tenant_name VARCHAR(255) NOT NULL,
                provider VARCHAR(50) NOT NULL,
                access_token TEXT NOT NULL,
                refresh_token TEXT,
                expires_at BIGINT NOT NULL,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

    // Create ApprovalMax tokens table
    await pool.query(`
            CREATE TABLE IF NOT EXISTS approvalmax_tokens (
                id SERIAL PRIMARY KEY,
                integration_key VARCHAR(255) UNIQUE NOT NULL,
                access_token TEXT NOT NULL,
                refresh_token TEXT,
                expires_at BIGINT NOT NULL,
                organizations JSONB,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

    console.error("✅ Database tables initialized successfully");
  } catch (error) {
    console.error("❌ Error initializing database:", error);
  }
}

// Database token storage functions
const tokenStorage = {
  // Store Xero token
  async storeXeroToken(tenantId, tenantName, tokenData) {
    try {
      await pool.query(
        `
    INSERT INTO tokens (tenant_id, tenant_name, provider, access_token, refresh_token, expires_at, last_seen)
    VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
    ON CONFLICT (tenant_id) 
    DO UPDATE SET 
        access_token = $4,
        refresh_token = $5,
        expires_at = $6,
        last_seen = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP
`,
        [
          tenantId,
          tenantName,
          "xero",
          tokenData.access_token,
          tokenData.refresh_token,
          Date.now() + tokenData.expires_in * 1000,
        ]
      );
      console.log(`✅ Stored Xero token for: ${tenantName}`);
    } catch (error) {
      console.error("❌ Error storing Xero token:", error);
    }
  },

  // Get Xero token
  async getXeroToken(tenantId) {
    try {
      const result = await pool.query(
        "SELECT * FROM tokens WHERE tenant_id = $1 AND provider = $2",
        [tenantId, "xero"]
      );

      if (result.rows.length === 0) {
        return null;
      }

      const token = result.rows[0];

      // Check if token is expired
      if (Date.now() > token.expires_at) {
        console.log(`⚠️ Token expired for tenant: ${tenantId}`);
        return null;
      }

      return {
        access_token: token.access_token,
        refresh_token: token.refresh_token,
        expires_in: Math.floor((token.expires_at - Date.now()) / 1000),
        tenantId: token.tenant_id,
        tenantName: token.tenant_name,
      };
    } catch (error) {
      console.error("❌ Error getting Xero token:", error);
      return null;
    }
  },

  // Get all Xero connections
  async getAllXeroConnections() {
    try {
      const result = await pool.query(
        "SELECT tenant_id, tenant_name, provider, expires_at, last_seen FROM tokens WHERE provider = $1",
        ["xero"]
      );

      return result.rows.map((row) => ({
        tenantId: row.tenant_id,
        tenantName: row.tenant_name,
        provider: row.provider,
        connected: Date.now() < row.expires_at,
        lastSeen: row.last_seen.toISOString(),
        error: Date.now() > row.expires_at ? "Token expired" : null,
      }));
    } catch (error) {
      console.error("❌ Error getting Xero connections:", error);
      return [];
    }
  },

  // Store ApprovalMax token
  async storeApprovalMaxToken(tokenData, organizations) {
    try {
      await pool.query(
        `
                INSERT INTO approvalmax_tokens (integration_key, access_token, refresh_token, expires_at, organizations, last_seen)
                VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
                ON CONFLICT (integration_key)
                DO UPDATE SET 
                    access_token = $2,
                    refresh_token = $3,
                    expires_at = $4,
                    organizations = $5,
                    last_seen = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
            `,
        [
          "approvalmax_integration",
          tokenData.access_token,
          tokenData.refresh_token,
          Date.now() + tokenData.expires_in * 1000,
          JSON.stringify(organizations),
        ]
      );
      console.log(
        `✅ Stored ApprovalMax token for ${organizations.length} organizations`
      );
    } catch (error) {
      console.error("❌ Error storing ApprovalMax token:", error);
    }
  },

  // Get ApprovalMax token
  async getApprovalMaxToken() {
    try {
      const result = await pool.query(
        "SELECT * FROM approvalmax_tokens WHERE integration_key = $1",
        ["approvalmax_integration"]
      );

      if (result.rows.length === 0) {
        return null;
      }

      const token = result.rows[0];

      // Check if token is expired
      if (Date.now() > token.expires_at) {
        console.error("⚠️ ApprovalMax token expired");
        return null;
      }

      return {
        accessToken: token.access_token,
        refreshToken: token.refresh_token,
        expiresAt: token.expires_at,
        organizations: token.organizations,
        lastSeen: token.last_seen.toISOString(),
      };
    } catch (error) {
      console.error("❌ Error getting ApprovalMax token:", error);
      return null;
    }
  },
};

// Middleware
app.use(express.static(path.join(__dirname, "public")));
app.use(express.json());

// Initialize Xero client with reports scope
const xero = new XeroClient({
  clientId: XERO_CLIENT_ID,
  clientSecret: XERO_CLIENT_SECRET,
  redirectUris: [XERO_REDIRECT_URI],
  scopes: [
    "accounting.transactions",
    "accounting.contacts",
    "accounting.settings",
    "accounting.reports.read",
    "offline_access", // ADD THIS LINE
    "accounting.budgets.read", // ADD THIS LINE
  ],
});

// Utility functions
function generateState() {
  return (
    Math.random().toString(36).substring(2, 15) +
    Math.random().toString(36).substring(2, 15)
  );
}

// ============================================================================
// XERO ROUTES (UPDATED WITH DATABASE STORAGE)
// ============================================================================

// Xero OAuth authorization
app.get("/auth", async (req, res) => {
  try {
    const provider = req.query.provider;

    if (provider === "approvalmax") {
      // Redirect to ApprovalMax OAuth
      const state = generateState();
      const authUrl = new URL(APPROVALMAX_CONFIG.authUrl);
      authUrl.searchParams.set("response_type", "code");
      authUrl.searchParams.set("client_id", APPROVALMAX_CLIENT_ID);
      authUrl.searchParams.set("scope", APPROVALMAX_CONFIG.scopes.join(" "));
      authUrl.searchParams.set("redirect_uri", APPROVALMAX_REDIRECT_URI);
      authUrl.searchParams.set("state", state);

      console.error("🎯 Redirecting to ApprovalMax OAuth:", authUrl.toString());
      res.redirect(authUrl.toString());
    } else {
      // Existing Xero OAuth
      const consentUrl = await xero.buildConsentUrl();
      console.error("🎯 Redirecting to Xero OAuth:", consentUrl);
      res.redirect(consentUrl);
    }
  } catch (error) {
    console.error("❌ Error in /auth:", error);
    res
      .status(500)
      .json({ error: "Authorization failed", details: error.message });
  }
});

// Xero OAuth callback - UPDATED WITH DATABASE STORAGE
app.get("/callback", async (req, res) => {
  try {
    const { code, state, error } = req.query;

    if (error) {
      console.error("❌ OAuth error:", error);
      return res.redirect("/?error=oauth_failed");
    }

    if (!code) {
      console.error("❌ No authorization code received");
      return res.redirect("/?error=no_code");
    }

    console.error("🔄 Processing Xero callback...");
    const tokenSet = await xero.apiCallback(req.url);

    if (!tokenSet || !tokenSet.access_token) {
      console.error("❌ No access token received from Xero");
      return res.redirect("/?error=no_token");
    }

    // Get tenant information
    const tenants = await xero.updateTenants(false, tokenSet);
    console.error("✅ Xero tenants received:", tenants.length);

    // Store tokens in database (instead of memory)
    for (const tenant of tenants) {
      await tokenStorage.storeXeroToken(
        tenant.tenantId,
        tenant.tenantName,
        tokenSet
      );
    }

    console.log(
      "✅ Xero tokens stored in database for",
      tenants.length,
      "tenants"
    );
    res.redirect("/?success=xero_connected");
  } catch (error) {
    console.error("❌ Error in Xero callback:", error);
    res.redirect("/?error=callback_failed");
  }
});

// ============================================================================
// APPROVALMAX ROUTES (UPDATED WITH DATABASE STORAGE)
// ============================================================================

// ApprovalMax OAuth callback - UPDATED WITH DATABASE STORAGE
app.get("/callback/approvalmax", async (req, res) => {
  try {
    const { code, state, error } = req.query;

    console.error("🎯 ApprovalMax callback received:", {
      code: code?.substring(0, 20) + "...",
      state,
      error,
    });

    if (error) {
      console.error("❌ ApprovalMax OAuth error:", error);
      return res.redirect("/?error=approvalmax_oauth_failed");
    }

    if (!code) {
      console.error("❌ No authorization code received from ApprovalMax");
      return res.redirect("/?error=approvalmax_no_code");
    }

    console.error("🔄 Exchanging ApprovalMax authorization code for tokens...");

    const redirectUri =
      APPROVALMAX_REDIRECT_URI ||
      "https://rac-financial-dashboard-production.up.railway.app/callback/approvalmax";

    const tokenRequestBody = {
      grant_type: "authorization_code",
      client_id: APPROVALMAX_CLIENT_ID,
      client_secret: APPROVALMAX_CLIENT_SECRET,
      redirect_uri: redirectUri,
      code: code,
    };

    const tokenResponse = await fetch(APPROVALMAX_CONFIG.tokenUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Accept: "application/json",
      },
      body: new URLSearchParams(tokenRequestBody),
    });

    const tokenData = await tokenResponse.json();

    if (!tokenResponse.ok || !tokenData.access_token) {
      console.error("❌ ApprovalMax token exchange failed:", {
        status: tokenResponse.status,
        error: tokenData.error,
        description: tokenData.error_description,
      });
      return res.redirect(
        `/?error=approvalmax_token_failed&details=${encodeURIComponent(
          tokenData.error || "Unknown error"
        )}`
      );
    }

    console.error("✅ ApprovalMax tokens received successfully");

    // Get organization information
    console.error("🔄 Fetching ApprovalMax organizations...");
    const orgsResponse = await fetch(`${APPROVALMAX_CONFIG.apiUrl}/companies`, {
      headers: {
        Authorization: `Bearer ${tokenData.access_token}`,
        Accept: "application/json",
      },
    });

    let organizations = [];
    if (orgsResponse.ok) {
      organizations = await orgsResponse.json();
      console.error(
        "✅ ApprovalMax organizations received:",
        organizations.length
      );
    } else {
      console.warn("⚠️ Failed to fetch organizations:", orgsResponse.status);
    }

    // Store tokens in database (instead of memory)
    await tokenStorage.storeApprovalMaxToken(tokenData, organizations);

    console.error(
      "✅ ApprovalMax tokens stored in database for",
      organizations.length,
      "organizations"
    );
    res.redirect("/?success=approvalmax_connected");
  } catch (error) {
    console.error("❌ Error in ApprovalMax callback:", error);
    res.redirect("/?error=approvalmax_callback_failed");
  }
});

// ============================================================================
// API ROUTES (UPDATED WITH DATABASE TOKEN RETRIEVAL)
// ============================================================================

// Connection status endpoint - UPDATED WITH DATABASE
app.get("/api/connection-status", async (req, res) => {
  try {
    const connections = [];

    // Get Xero connections from database
    const xeroConnections = await tokenStorage.getAllXeroConnections();
    connections.push(...xeroConnections);

    // Get ApprovalMax connections from database
    const approvalMaxToken = await tokenStorage.getApprovalMaxToken();
    if (approvalMaxToken) {
      connections.push({
        tenantId: "approvalmax_integration",
        tenantName: "RAC ApprovalMax Integration",
        provider: "approvalmax",
        connected: true,
        lastSeen: approvalMaxToken.lastSeen,
        organizationCount: approvalMaxToken.organizations
          ? approvalMaxToken.organizations.length
          : 0,
        error: null,
      });
    }

    console.error(
      "📊 Connection status from database:",
      connections.length,
      "total connections"
    );
    res.json(connections);
  } catch (error) {
    console.error("❌ Error getting connection status:", error);
    res.status(500).json({ error: "Failed to get connection status" });
  }
});

// FIXED: Cash position endpoint with DATABASE token retrieval
app.get("/api/cash-position/:tenantId", async (req, res) => {
  try {
    // Get token from database instead of memory
    const tokenData = await tokenStorage.getXeroToken(req.params.tenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    await xero.setTokenSet(tokenData);

    const response = await xero.accountingApi.getAccounts(
      req.params.tenantId,
      null,
      'Type=="BANK"'
    );
    const bankAccounts = response.body.accounts || [];

    // FIXED: Use CurrentBalance instead of runningBalance
    const totalCash = bankAccounts.reduce((sum, account) => {
      return sum + (parseFloat(account.CurrentBalance) || 0);
    }, 0);

    res.json({
      totalCash,
      bankAccounts: bankAccounts.map((acc) => ({
        name: acc.name,
        balance: parseFloat(acc.CurrentBalance) || 0,
        code: acc.code,
      })),
    });
  } catch (error) {
    console.error("❌ Error getting cash position:", error);
    res.status(500).json({ error: "Failed to get cash position" });
  }
});

// FIXED: Receivables endpoint with DATABASE token retrieval
app.get("/api/receivables/:tenantId", async (req, res) => {
  try {
    // Get token from database instead of memory
    const tokenData = await tokenStorage.getXeroToken(req.params.tenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    await xero.setTokenSet(tokenData);

    const response = await xero.accountingApi.getAccounts(
      req.params.tenantId,
      null,
      'Type=="RECEIVABLE"'
    );
    const receivableAccounts = response.body.accounts || [];

    // FIXED: Use CurrentBalance instead of runningBalance
    const totalReceivables = receivableAccounts.reduce((sum, account) => {
      return sum + (parseFloat(account.CurrentBalance) || 0);
    }, 0);

    res.json({ totalReceivables });
  } catch (error) {
    console.error("❌ Error getting receivables:", error);
    res.status(500).json({ error: "Failed to get receivables" });
  }
});

// Outstanding invoices endpoint - UPDATED WITH DATABASE
app.get("/api/outstanding-invoices/:tenantId", async (req, res) => {
  try {
    // Get token from database instead of memory
    const tokenData = await tokenStorage.getXeroToken(req.params.tenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    await xero.setTokenSet(tokenData);

    const response = await xero.accountingApi.getInvoices(
      req.params.tenantId,
      null,
      null,
      'Status=="AUTHORISED"'
    );
    const invoices = response.body.invoices || [];

    const outstandingInvoices = invoices.filter(
      (inv) => inv.status === "AUTHORISED" && parseFloat(inv.amountDue) > 0
    );

    res.json(
      outstandingInvoices.map((inv) => ({
        invoiceID: inv.invoiceID,
        invoiceNumber: inv.invoiceNumber,
        contact: inv.contact?.name,
        amountDue: parseFloat(inv.amountDue),
        total: parseFloat(inv.total),
        date: inv.date,
        dueDate: inv.dueDate,
      }))
    );
  } catch (error) {
    console.error("❌ Error getting outstanding invoices:", error);
    res.status(500).json({ error: "Failed to get outstanding invoices" });
  }
});

// Contacts endpoint - UPDATED WITH DATABASE
app.get("/api/contacts/:tenantId", async (req, res) => {
  try {
    // Get token from database instead of memory
    const tokenData = await tokenStorage.getXeroToken(req.params.tenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    await xero.setTokenSet(tokenData);

    const response = await xero.accountingApi.getContacts(req.params.tenantId);
    const contacts = response.body.contacts || [];

    res.json(
      contacts.map((contact) => ({
        contactID: contact.contactID,
        name: contact.name,
        isCustomer: contact.isCustomer,
        isSupplier: contact.isSupplier,
        emailAddress: contact.emailAddress,
      }))
    );
  } catch (error) {
    console.error("❌ Error getting contacts:", error);
    res.status(500).json({ error: "Failed to get contacts" });
  }
});

// ApprovalMax companies endpoint - UPDATED WITH DATABASE
app.get("/api/approvalmax/companies", async (req, res) => {
  try {
    const tokenData = await tokenStorage.getApprovalMaxToken();
    if (!tokenData) {
      return res.status(404).json({ error: "ApprovalMax not connected" });
    }

    const response = await fetch(`${APPROVALMAX_CONFIG.apiUrl}/companies`, {
      headers: {
        Authorization: `Bearer ${tokenData.accessToken}`,
        Accept: "application/json",
      },
    });

    if (!response.ok) {
      throw new Error(`ApprovalMax API error: ${response.status}`);
    }

    const companies = await response.json();
    res.json(companies);
  } catch (error) {
    console.error("❌ Error getting ApprovalMax companies:", error);
    res.status(500).json({ error: "Failed to get companies" });
  }
});

// Consolidated data endpoint - UPDATED WITH DATABASE
app.get("/api/consolidated", async (req, res) => {
  try {
    console.error("🔄 Loading consolidated data from database...");

    let totalCash = 0;
    let totalReceivables = 0;
    let totalOutstandingInvoices = 0;
    let tenantData = [];

    // Get all Xero connections from database
    const xeroConnections = await tokenStorage.getAllXeroConnections();
    const connectedXeroEntities = xeroConnections.filter(
      (conn) => conn.connected
    );

    // Aggregate Xero data
    for (const connection of connectedXeroEntities) {
      try {
        const [cashResponse, receivablesResponse, invoicesResponse] =
          await Promise.all([
            fetch(
              `${req.protocol}://${req.get("host")}/api/cash-position/${
                connection.tenantId
              }`
            ),
            fetch(
              `${req.protocol}://${req.get("host")}/api/receivables/${
                connection.tenantId
              }`
            ),
            fetch(
              `${req.protocol}://${req.get("host")}/api/outstanding-invoices/${
                connection.tenantId
              }`
            ),
          ]);

        if (cashResponse.ok && receivablesResponse.ok && invoicesResponse.ok) {
          const [cashData, receivablesData, invoicesData] = await Promise.all([
            cashResponse.json(),
            receivablesResponse.json(),
            invoicesResponse.json(),
          ]);

          totalCash += cashData.totalCash || 0;
          totalReceivables += receivablesData.totalReceivables || 0;
          totalOutstandingInvoices += invoicesData.length || 0;

          tenantData.push({
            tenantId: connection.tenantId,
            tenantName: connection.tenantName,
            provider: "xero",
            cashPosition: cashData.totalCash || 0,
            receivables: receivablesData.totalReceivables || 0,
            outstandingInvoices: invoicesData.length || 0,
            bankAccounts: cashData.bankAccounts || [],
          });
        }
      } catch (error) {
        console.error(
          `❌ Error loading data for tenant ${connection.tenantId}:`,
          error
        );
      }
    }

    // Add ApprovalMax data
    let totalPendingApprovals = 0;
    let totalApprovalValue = 0;
    let approvalData = [];

    const amTokenData = await tokenStorage.getApprovalMaxToken();
    if (amTokenData) {
      try {
        const summaryResponse = await fetch(
          `${req.protocol}://${req.get(
            "host"
          )}/api/approvalmax/approval-summary/integration`
        );
        if (summaryResponse.ok) {
          const summaryData = await summaryResponse.json();
          totalPendingApprovals = summaryData.pendingApprovals || 0;
          totalApprovalValue = summaryData.totalValue || 0;

          approvalData.push({
            organizationId: "integration",
            organizationName: "RAC ApprovalMax Integration",
            provider: "approvalmax",
            pendingApprovals: summaryData.pendingApprovals || 0,
            totalValue: summaryData.totalValue || 0,
            organizationCount: summaryData.organizationCount || 0,
          });
        }
      } catch (error) {
        console.error("❌ Error loading ApprovalMax data:", error);
      }
    }

    const consolidatedData = {
      totalCash,
      totalReceivables,
      totalOutstandingInvoices,
      totalPendingApprovals,
      totalApprovalValue,
      tenantData,
      approvalData,
      lastUpdated: new Date().toISOString(),
    };

    console.error("✅ Consolidated data loaded from database:", {
      xeroEntities: tenantData.length,
      approvalMaxOrgs: approvalData.length,
      totalCash,
      totalReceivables,
    });

    res.json(consolidatedData);
  } catch (error) {
    console.error("❌ Error loading consolidated data:", error);
    res.status(500).json({ error: "Failed to load consolidated data" });
  }
});

// AUTO TOKEN REFRESH SYSTEM
// Add these functions to your server.js

// Enhanced token storage with refresh capability
const enhancedTokenStorage = {
  ...tokenStorage, // Keep all existing functions

  // Refresh a specific Xero token
  async refreshXeroToken(tenantId) {
    try {
      console.log(`🔄 Attempting to refresh token for tenant: ${tenantId}`);

      // Get current token from database
      const result = await pool.query(
        "SELECT * FROM tokens WHERE tenant_id = $1 AND provider = $2",
        [tenantId, "xero"]
      );

      if (result.rows.length === 0) {
        console.log(`❌ No token found for tenant: ${tenantId}`);
        return { success: false, error: "Token not found" };
      }

      const storedToken = result.rows[0];

      if (!storedToken.refresh_token) {
        console.log(`❌ No refresh token available for tenant: ${tenantId}`);
        return { success: false, error: "No refresh token" };
      }

      // Use Xero SDK to refresh the token
      const tokenSet = {
        access_token: storedToken.access_token,
        refresh_token: storedToken.refresh_token,
        expires_in: Math.floor((storedToken.expires_at - Date.now()) / 1000),
      };

      await xero.setTokenSet(tokenSet);

      // Refresh the token
      const newTokenSet = await xero.refreshToken();
      console.log(
        `✅ Token refreshed successfully for: ${storedToken.tenant_name}`
      );

      // Store the new token in database
      await pool.query(
        `UPDATE tokens 
         SET access_token = $1, 
             refresh_token = $2, 
             expires_at = $3, 
             last_seen = CURRENT_TIMESTAMP,
             updated_at = CURRENT_TIMESTAMP
         WHERE tenant_id = $4 AND provider = $5`,
        [
          newTokenSet.access_token,
          newTokenSet.refresh_token,
          Date.now() + newTokenSet.expires_in * 1000,
          tenantId,
          "xero",
        ]
      );

      return {
        success: true,
        newExpiresAt: Date.now() + newTokenSet.expires_in * 1000,
        tenantName: storedToken.tenant_name,
      };
    } catch (error) {
      console.error(`❌ Error refreshing token for ${tenantId}:`, error);
      return {
        success: false,
        error: error.message,
        requiresReauth:
          error.message?.includes("invalid_grant") ||
          error.message?.includes("unauthorized"),
      };
    }
  },

  // Refresh all Xero tokens that are close to expiring
  async refreshAllExpiringTokens() {
    try {
      console.error("🔄 Checking for tokens that need refresh...");

      // Get tokens that expire in the next 10 minutes
      const tenMinutesFromNow = Date.now() + 10 * 60 * 1000;

      const result = await pool.query(
        `SELECT tenant_id, tenant_name, expires_at 
         FROM tokens 
         WHERE provider = $1 
         AND expires_at < $2 
         AND expires_at > $3`,
        ["xero", tenMinutesFromNow, Date.now()]
      );

      if (result.rows.length === 0) {
        console.error("✅ No tokens need refreshing");
        return { refreshed: 0, failed: 0, results: [] };
      }

      const refreshResults = [];
      let refreshed = 0;
      let failed = 0;

      // Refresh each token
      for (const token of result.rows) {
        const result = await this.refreshXeroToken(token.tenant_id);
        refreshResults.push({
          tenantId: token.tenant_id,
          tenantName: token.tenant_name,
          ...result,
        });

        if (result.success) {
          refreshed++;
          console.log(`✅ Refreshed: ${token.tenant_name}`);
        } else {
          failed++;
          console.log(
            `❌ Failed to refresh: ${token.tenant_name} - ${result.error}`
          );
        }
      }

      return { refreshed, failed, results: refreshResults };
    } catch (error) {
      console.error("❌ Error in refreshAllExpiringTokens:", error);
      return { refreshed: 0, failed: 0, error: error.message };
    }
  },

  // Get tokens that will expire soon (for frontend warning)
  async getExpiringTokens(minutesAhead = 15) {
    try {
      const futureTime = Date.now() + minutesAhead * 60 * 1000;

      const result = await pool.query(
        `SELECT tenant_id, tenant_name, expires_at 
         FROM tokens 
         WHERE provider = $1 
         AND expires_at < $2 
         AND expires_at > $3
         ORDER BY expires_at ASC`,
        ["xero", futureTime, Date.now()]
      );

      return result.rows.map((row) => ({
        tenantId: row.tenant_id,
        tenantName: row.tenant_name,
        expiresAt: row.expires_at,
        minutesUntilExpiry: Math.floor(
          (row.expires_at - Date.now()) / (1000 * 60)
        ),
      }));
    } catch (error) {
      console.error("❌ Error getting expiring tokens:", error);
      return [];
    }
  },
};

// Auto-refresh scheduler - runs every 5 minutes
let autoRefreshInterval;

function startAutoRefresh() {
  console.error("🚀 Starting auto token refresh system...");

  // Run immediately
  enhancedTokenStorage.refreshAllExpiringTokens();

  // Then run every 5 minutes
  autoRefreshInterval = setInterval(async () => {
    const result = await enhancedTokenStorage.refreshAllExpiringTokens();

    if (result.refreshed > 0) {
      console.error(
        `🔄 Auto-refresh completed: ${result.refreshed} tokens refreshed, ${result.failed} failed`
      );
    }
  }, 5 * 60 * 1000); // Every 5 minutes
}

function stopAutoRefresh() {
  if (autoRefreshInterval) {
    clearInterval(autoRefreshInterval);
    autoRefreshInterval = null;
    console.error("⏹️ Auto token refresh stopped");
  }
}

// API endpoint to check token status and warnings
app.get("/api/token-status", async (req, res) => {
  try {
    const [allConnections, expiringTokens] = await Promise.all([
      enhancedTokenStorage.getAllXeroConnections(),
      enhancedTokenStorage.getExpiringTokens(15), // Warn 15 minutes ahead
    ]);

    const tokenStatus = {
      totalTokens: allConnections.length,
      connectedTokens: allConnections.filter((conn) => conn.connected).length,
      expiredTokens: allConnections.filter((conn) => !conn.connected).length,
      expiringTokens: expiringTokens.length,
      expiringDetails: expiringTokens,
      needsAttention:
        expiringTokens.length > 0 ||
        allConnections.filter((conn) => !conn.connected).length > 0,
    };

    res.json(tokenStatus);
  } catch (error) {
    console.error("❌ Token status error:", error);
    res.status(500).json({ error: error.message });
  }
});

// Enhanced connection status with auto-refresh info
app.get("/api/connection-status-enhanced", async (req, res) => {
  try {
    const [connections, expiringTokens] = await Promise.all([
      enhancedTokenStorage.getAllXeroConnections(),
      enhancedTokenStorage.getExpiringTokens(30), // Check 30 minutes ahead
    ]);

    // Add expiry warnings to connection data
    const enhancedConnections = connections.map((conn) => {
      const expiring = expiringTokens.find(
        (exp) => exp.tenantId === conn.tenantId
      );
      return {
        ...conn,
        minutesUntilExpiry: expiring ? expiring.minutesUntilExpiry : null,
        needsRefresh: expiring ? expiring.minutesUntilExpiry < 15 : false,
      };
    });

    // Add ApprovalMax connections (keep existing logic)
    const approvalMaxToken = await enhancedTokenStorage.getApprovalMaxToken();
    if (approvalMaxToken) {
      enhancedConnections.push({
        tenantId: "approvalmax_integration",
        tenantName: "RAC ApprovalMax Integration",
        provider: "approvalmax",
        connected: true,
        lastSeen: approvalMaxToken.lastSeen,
        organizationCount: approvalMaxToken.organizations
          ? approvalMaxToken.organizations.length
          : 0,
        error: null,
        minutesUntilExpiry: null,
        needsRefresh: false,
      });
    }

    res.json(enhancedConnections);
  } catch (error) {
    console.error("❌ Error getting enhanced connection status:", error);
    res.status(500).json({ error: "Failed to get enhanced connection status" });
  }
});

// GET Budgets - CORRECTED
app.post("/api/budgets", async (req, res) => {
  try {
    const { tenantId, organizationName, budgetId } = req.body;

    if (!organizationName && !tenantId) {
      return res
        .status(400)
        .json({ error: "Organization name or tenant ID required" });
    }

    // Find tenant ID if organization name provided (SAME AS YOUR OTHER ENDPOINTS)
    let actualTenantId = tenantId;
    if (organizationName && !tenantId) {
      const connections = await tokenStorage.getAllXeroConnections();
      const connection = connections.find((c) =>
        c.tenantName.toLowerCase().includes(organizationName.toLowerCase())
      );
      if (connection) {
        actualTenantId = connection.tenantId;
      } else {
        return res.status(404).json({ error: "Organization not found" });
      }
    }

    // Get token (SAME AS YOUR OTHER ENDPOINTS)
    const tokenData = await tokenStorage.getXeroToken(actualTenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    await xero.setTokenSet(tokenData);

    // Call Xero Budgets API
    let budgets;
    if (budgetId) {
      budgets = await xero.accountingApi.getBudget(actualTenantId, budgetId);
    } else {
      budgets = await xero.accountingApi.getBudgets(actualTenantId);
    }

    res.json({
      tenantId: actualTenantId,
      budgets: budgets.body.budgets || [],
      generatedAt: new Date().toISOString(),
    });
  } catch (error) {
    console.error("Budget API Error:", error);
    res.status(500).json({ error: error.message });
  }
});

// GET Budget Summary Report - CORRECTED
app.post("/api/budget-summary", async (req, res) => {
  try {
    const { tenantId, organizationName, date, periods } = req.body;

    if (!organizationName && !tenantId) {
      return res
        .status(400)
        .json({ error: "Organization name or tenant ID required" });
    }

    // Find tenant ID if organization name provided
    let actualTenantId = tenantId;
    if (organizationName && !tenantId) {
      const connections = await tokenStorage.getAllXeroConnections();
      const connection = connections.find((c) =>
        c.tenantName.toLowerCase().includes(organizationName.toLowerCase())
      );
      if (connection) {
        actualTenantId = connection.tenantId;
      } else {
        return res.status(404).json({ error: "Organization not found" });
      }
    }

    // Get token
    const tokenData = await tokenStorage.getXeroToken(actualTenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    await xero.setTokenSet(tokenData);

    // Call Budget Summary Report - REMOVE timeframe parameter
    const report = await xero.accountingApi.getReportBudgetSummary(
      actualTenantId,
      date,
      periods || 12
    );

    res.json({
      tenantId: actualTenantId,
      report: report.body.reports[0],
      generatedAt: new Date().toISOString(),
    });
  } catch (error) {
    console.error("Budget Summary Error:", error);
    res.status(500).json({ error: error.message });
  }
});

// add post here Duane
// === REST API Endpoints for Web Chat Interface ===
// API endpoint to manually trigger refresh
app.post("/api/refresh-tokens", async (req, res) => {
  try {
    console.error("🔄 Manual token refresh requested");
    const result = await enhancedTokenStorage.refreshAllExpiringTokens();

    res.json({
      success: true,
      refreshed: result.refreshed,
      failed: result.failed,
      results: result.results,
      message: `Refreshed ${result.refreshed} tokens, ${result.failed} failed`,
    });
  } catch (error) {
    console.error("❌ Manual refresh error:", error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// POST Trial Balance endpoint (for web chat interface)
app.post("/api/trial-balance", async (req, res) => {
  try {
    const { organizationName, tenantId, reportDate } = req.body;

    if (!organizationName && !tenantId) {
      return res
        .status(400)
        .json({ error: "Organization name or tenant ID required" });
    }

    // Find tenant ID if organization name provided
    let actualTenantId = tenantId;
    if (organizationName && !tenantId) {
      const connections = await tokenStorage.getAllXeroConnections();
      const connection = connections.find((c) =>
        c.tenantName.toLowerCase().includes(organizationName.toLowerCase())
      );
      if (connection) {
        actualTenantId = connection.tenantId;
      } else {
        return res.status(404).json({ error: "Organization not found" });
      }
    }

    // Call your existing GET endpoint internally
    const dateParam = reportDate ? `?date=${reportDate}` : "";
    const trialBalanceUrl = `${req.protocol}://${req.get(
      "host"
    )}/api/trial-balance/${actualTenantId}${dateParam}`;
    const response = await fetch(trialBalanceUrl);

    if (!response.ok) {
      throw new Error(`Trial balance request failed: ${response.status}`);
    }

    const result = await response.json();
    res.json(result);
  } catch (error) {
    console.error("❌ Trial balance POST API error:", error);
    res.status(500).json({ error: error.message });
  }
});

app.post("/api/cash-position", async (req, res) => {
  try {
    const { organizationName, tenantId } = req.body;

    if (!organizationName && !tenantId) {
      return res
        .status(400)
        .json({ error: "Organization name or tenant ID required" });
    }

    // Find tenant ID if organization name provided
    let actualTenantId = tenantId;
    if (organizationName && !tenantId) {
      const connections = await tokenStorage.getAllXeroConnections();
      const connection = connections.find((c) =>
        c.tenantName.toLowerCase().includes(organizationName.toLowerCase())
      );
      if (connection) {
        actualTenantId = connection.tenantId;
      } else {
        return res.status(404).json({ error: "Organization not found" });
      }
    }

    // Get token and call Bank Summary report
    const tokenData = await tokenStorage.getXeroToken(actualTenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    await xero.setTokenSet(tokenData);

    // Use Bank Summary Report API for current balances
    const response = await xero.accountingApi.getReportBankSummary(
      actualTenantId
    );

    // Replace the existing Bank Summary parsing with this:
    const bankSummaryRows = response.body.reports?.[0]?.rows || [];
    const bankAccounts = [];
    let totalCash = 0;

    // Find the Section that contains the bank account rows
    bankSummaryRows.forEach((row) => {
      if (row.rowType === "Section" && row.rows) {
        // Loop through each bank account row in the section
        row.rows.forEach((bankRow) => {
          if (
            bankRow.rowType === "Row" &&
            bankRow.cells &&
            bankRow.cells.length >= 5
          ) {
            const accountName = bankRow.cells[0]?.value || "";
            const closingBalance = parseFloat(bankRow.cells[4]?.value || 0); // Cell[4] = Closing Balance
            const accountId =
              bankRow.cells[0]?.attributes?.find(
                (attr) => attr.id === "accountID"
              )?.value || "";

            if (accountName && !accountName.toLowerCase().includes("total")) {
              bankAccounts.push({
                name: accountName,
                balance: closingBalance,
                code: accountId,
              });
              totalCash += closingBalance;
            }
          }
        });
      }
    });

    res.json({
      totalCash,
      bankAccounts,
    });
  } catch (error) {
    console.error("❌ Cash position API error:", error);
    res.status(500).json({ error: error.message });
  }
});

// Also update the POST endpoint for P&L
app.post("/api/profit-loss-summary", async (req, res) => {
  try {
    const { organizationName, tenantId, date, periodMonths } = req.body;

    if (!organizationName && !tenantId) {
      return res
        .status(400)
        .json({ error: "Organization name or tenant ID required" });
    }

    // Find tenant ID if organization name provided
    let actualTenantId = tenantId;
    if (organizationName && !tenantId) {
      const connections = await tokenStorage.getAllXeroConnections();
      const connection = connections.find((c) =>
        c.tenantName.toLowerCase().includes(organizationName.toLowerCase())
      );
      if (connection) {
        actualTenantId = connection.tenantId;
      } else {
        return res.status(404).json({ error: "Organization not found" });
      }
    }

    // Build parameters with updated defaults
    const params = new URLSearchParams();
    if (date) params.append("date", date);
    // Default to 1 month instead of 12 for current month behavior
    params.append("periodMonths", (periodMonths || 1).toString());
    const queryString = params.toString()
      ? `?${params.toString()}`
      : "?periodMonths=1";

    const response = await fetch(
      `${req.protocol}://${req.get(
        "host"
      )}/api/profit-loss/${actualTenantId}${queryString}`
    );

    if (!response.ok) {
      throw new Error(`P&L request failed: ${response.status}`);
    }

    const result = await response.json();
    res.json(result);
  } catch (error) {
    console.error("❌ P&L summary API error:", error);
    res.status(500).json({ error: error.message });
  }
});

// Outstanding Invoices endpoint
app.post("/api/outstanding-invoices", async (req, res) => {
  try {
    const { organizationName, tenantId } = req.body;

    if (!organizationName && !tenantId) {
      return res
        .status(400)
        .json({ error: "Organization name or tenant ID required" });
    }

    // Find tenant ID if organization name provided
    let actualTenantId = tenantId;
    if (organizationName && !tenantId) {
      const connections = await tokenStorage.getAllXeroConnections();
      const connection = connections.find((c) =>
        c.tenantName.toLowerCase().includes(organizationName.toLowerCase())
      );
      if (connection) {
        actualTenantId = connection.tenantId;
      } else {
        return res.status(404).json({ error: "Organization not found" });
      }
    }

    // Call your existing GET endpoint internally
    const response = await fetch(
      `${req.protocol}://${req.get(
        "host"
      )}/api/outstanding-invoices/${actualTenantId}`
    );

    if (!response.ok) {
      throw new Error(
        `Outstanding invoices request failed: ${response.status}`
      );
    }

    const result = await response.json();
    res.json(result);
  } catch (error) {
    console.error("❌ Outstanding invoices API error:", error);
    res.status(500).json({ error: error.message });
  }
});

// Financial Ratios endpoint
app.post("/api/financial-ratios", async (req, res) => {
  try {
    const { organizationName, tenantId, date } = req.body;

    if (!organizationName && !tenantId) {
      return res
        .status(400)
        .json({ error: "Organization name or tenant ID required" });
    }

    // Find tenant ID if organization name provided
    let actualTenantId = tenantId;
    if (organizationName && !tenantId) {
      const connections = await tokenStorage.getAllXeroConnections();
      const connection = connections.find((c) =>
        c.tenantName.toLowerCase().includes(organizationName.toLowerCase())
      );
      if (connection) {
        actualTenantId = connection.tenantId;
      } else {
        return res.status(404).json({ error: "Organization not found" });
      }
    }

    // Call your existing GET endpoint internally
    const dateParam = date ? `?date=${date}` : "";
    const response = await fetch(
      `${req.protocol}://${req.get(
        "host"
      )}/api/financial-ratios/${actualTenantId}${dateParam}`
    );

    if (!response.ok) {
      throw new Error(`Financial ratios request failed: ${response.status}`);
    }

    const result = await response.json();
    res.json(result);
  } catch (error) {
    console.error("❌ Financial ratios API error:", error);
    res.status(500).json({ error: error.message });
  }
});

// Consolidated Trial Balance endpoint
app.post("/api/consolidated-trial-balance", async (req, res) => {
  try {
    const { reportDate } = req.body;

    const dateParam = reportDate ? `?date=${reportDate}` : "";
    const response = await fetch(
      `${req.protocol}://${req.get(
        "host"
      )}/api/consolidated-trial-balance${dateParam}`
    );

    if (!response.ok) {
      throw new Error(
        `Consolidated trial balance request failed: ${response.status}`
      );
    }

    const result = await response.json();
    res.json(result);
  } catch (error) {
    console.error("❌ Consolidated trial balance API error:", error);
    res.status(500).json({ error: error.message });
  }
});

// Journal Entries endpoint
app.post("/api/journal-entries", async (req, res) => {
  try {
    const { organizationName, tenantId, dateFrom, dateTo, accountName } =
      req.body;

    if (!organizationName && !tenantId) {
      return res
        .status(400)
        .json({ error: "Organization name or tenant ID required" });
    }

    let actualTenantId = tenantId;
    if (organizationName && !tenantId) {
      const connections = await tokenStorage.getAllXeroConnections();
      const connection = connections.find((c) =>
        c.tenantName.toLowerCase().includes(organizationName.toLowerCase())
      );
      if (connection) {
        actualTenantId = connection.tenantId;
      } else {
        return res.status(404).json({ error: "Organization not found" });
      }
    }

    const params = new URLSearchParams();
    if (dateFrom) params.append("dateFrom", dateFrom);
    if (dateTo) params.append("dateTo", dateTo);
    if (accountName) params.append("accountName", accountName);
    const queryString = params.toString() ? `?${params.toString()}` : "";

    const response = await fetch(
      `${req.protocol}://${req.get(
        "host"
      )}/api/journal-entries/${actualTenantId}${queryString}`
    );

    if (!response.ok) {
      throw new Error(`Journal entries request failed: ${response.status}`);
    }

    const result = await response.json();
    res.json(result);
  } catch (error) {
    console.error("❌ Journal entries API error:", error);
    res.status(500).json({ error: error.message });
  }
});

// Equity Analysis endpoint
app.post("/api/equity-analysis", async (req, res) => {
  try {
    const { organizationName, tenantId, equityAccountName, monthsBack } =
      req.body;

    if (!organizationName && !tenantId) {
      return res
        .status(400)
        .json({ error: "Organization name or tenant ID required" });
    }

    let actualTenantId = tenantId;
    if (organizationName && !tenantId) {
      const connections = await tokenStorage.getAllXeroConnections();
      const connection = connections.find((c) =>
        c.tenantName.toLowerCase().includes(organizationName.toLowerCase())
      );
      if (connection) {
        actualTenantId = connection.tenantId;
      } else {
        return res.status(404).json({ error: "Organization not found" });
      }
    }

    const params = new URLSearchParams();
    if (equityAccountName)
      params.append("equityAccountName", equityAccountName);
    if (monthsBack) params.append("monthsBack", monthsBack.toString());
    const queryString = params.toString() ? `?${params.toString()}` : "";

    const response = await fetch(
      `${req.protocol}://${req.get(
        "host"
      )}/api/equity-analysis/${actualTenantId}${queryString}`
    );

    if (!response.ok) {
      throw new Error(`Equity analysis request failed: ${response.status}`);
    }

    const result = await response.json();
    res.json(result);
  } catch (error) {
    console.error("❌ Equity analysis API error:", error);
    res.status(500).json({ error: error.message });
  }
});

// Account History endpoint
app.post("/api/account-history", async (req, res) => {
  try {
    const { organizationName, tenantId, accountName, dateFrom, dateTo } =
      req.body;

    if (!organizationName && !tenantId) {
      return res
        .status(400)
        .json({ error: "Organization name or tenant ID required" });
    }

    if (!accountName) {
      return res.status(400).json({ error: "Account name is required" });
    }

    let actualTenantId = tenantId;
    if (organizationName && !tenantId) {
      const connections = await tokenStorage.getAllXeroConnections();
      const connection = connections.find((c) =>
        c.tenantName.toLowerCase().includes(organizationName.toLowerCase())
      );
      if (connection) {
        actualTenantId = connection.tenantId;
      } else {
        return res.status(404).json({ error: "Organization not found" });
      }
    }

    const params = new URLSearchParams();
    if (dateFrom) params.append("dateFrom", dateFrom);
    if (dateTo) params.append("dateTo", dateTo);
    const queryString = params.toString() ? `?${params.toString()}` : "";

    const response = await fetch(
      `${req.protocol}://${req.get(
        "host"
      )}/api/account-history/${actualTenantId}/${encodeURIComponent(
        accountName
      )}${queryString}`
    );

    if (!response.ok) {
      throw new Error(`Account history request failed: ${response.status}`);
    }

    const result = await response.json();
    res.json(result);
  } catch (error) {
    console.error("❌ Account history API error:", error);
    res.status(500).json({ error: error.message });
  }
});

// Aged Receivables endpoint
app.post("/api/aged-receivables", async (req, res) => {
  try {
    const { organizationName, tenantId, date } = req.body;

    if (!organizationName && !tenantId) {
      return res
        .status(400)
        .json({ error: "Organization name or tenant ID required" });
    }

    let actualTenantId = tenantId;
    if (organizationName && !tenantId) {
      const connections = await tokenStorage.getAllXeroConnections();
      const connection = connections.find((c) =>
        c.tenantName.toLowerCase().includes(organizationName.toLowerCase())
      );
      if (connection) {
        actualTenantId = connection.tenantId;
      } else {
        return res.status(404).json({ error: "Organization not found" });
      }
    }

    const dateParam = date ? `?date=${date}` : "";
    const response = await fetch(
      `${req.protocol}://${req.get(
        "host"
      )}/api/aged-receivables/${actualTenantId}${dateParam}`
    );

    if (!response.ok) {
      throw new Error(`Aged receivables request failed: ${response.status}`);
    }

    const result = await response.json();
    res.json(result);
  } catch (error) {
    console.error("❌ Aged receivables API error:", error);
    res.status(500).json({ error: error.message });
  }
});

// Expense Analysis endpoint
app.post("/api/expense-analysis", async (req, res) => {
  try {
    const { organizationName, tenantId, date, periodMonths } = req.body;

    if (!organizationName && !tenantId) {
      return res
        .status(400)
        .json({ error: "Organization name or tenant ID required" });
    }

    let actualTenantId = tenantId;
    if (organizationName && !tenantId) {
      const connections = await tokenStorage.getAllXeroConnections();
      const connection = connections.find((c) =>
        c.tenantName.toLowerCase().includes(organizationName.toLowerCase())
      );
      if (connection) {
        actualTenantId = connection.tenantId;
      } else {
        return res.status(404).json({ error: "Organization not found" });
      }
    }

    const params = new URLSearchParams();
    if (date) params.append("date", date);
    if (periodMonths) params.append("periodMonths", periodMonths.toString());
    const queryString = params.toString() ? `?${params.toString()}` : "";

    const response = await fetch(
      `${req.protocol}://${req.get(
        "host"
      )}/api/expense-analysis/${actualTenantId}${queryString}`
    );

    if (!response.ok) {
      throw new Error(`Expense analysis request failed: ${response.status}`);
    }

    const result = await response.json();
    res.json(result);
  } catch (error) {
    console.error("❌ Expense analysis API error:", error);
    res.status(500).json({ error: error.message });
  }
});

// Intercompany Transactions endpoint
app.post("/api/intercompany-transactions", async (req, res) => {
  try {
    const { organizationName, tenantId, date } = req.body;

    if (!organizationName && !tenantId) {
      return res
        .status(400)
        .json({ error: "Organization name or tenant ID required" });
    }

    let actualTenantId = tenantId;
    if (organizationName && !tenantId) {
      const connections = await tokenStorage.getAllXeroConnections();
      const connection = connections.find((c) =>
        c.tenantName.toLowerCase().includes(organizationName.toLowerCase())
      );
      if (connection) {
        actualTenantId = connection.tenantId;
      } else {
        return res.status(404).json({ error: "Organization not found" });
      }
    }

    const dateParam = date ? `?date=${date}` : "";
    const response = await fetch(
      `${req.protocol}://${req.get(
        "host"
      )}/api/intercompany/${actualTenantId}${dateParam}`
    );

    if (!response.ok) {
      throw new Error(
        `Intercompany transactions request failed: ${response.status}`
      );
    }

    const result = await response.json();
    res.json(result);
  } catch (error) {
    console.error("❌ Intercompany transactions API error:", error);
    res.status(500).json({ error: error.message });
  }
});

// Chart of Accounts endpoint
app.post("/api/chart-of-accounts", async (req, res) => {
  try {
    const { organizationName, tenantId, accountType, includeArchived } =
      req.body;

    if (!organizationName && !tenantId) {
      return res
        .status(400)
        .json({ error: "Organization name or tenant ID required" });
    }

    let actualTenantId = tenantId;
    if (organizationName && !tenantId) {
      const connections = await tokenStorage.getAllXeroConnections();
      const connection = connections.find((c) =>
        c.tenantName.toLowerCase().includes(organizationName.toLowerCase())
      );
      if (connection) {
        actualTenantId = connection.tenantId;
      } else {
        return res.status(404).json({ error: "Organization not found" });
      }
    }

    const params = new URLSearchParams();
    if (accountType) params.append("accountType", accountType);
    if (includeArchived !== undefined)
      params.append("includeArchived", includeArchived.toString());
    const queryString = params.toString() ? `?${params.toString()}` : "";

    const response = await fetch(
      `${req.protocol}://${req.get(
        "host"
      )}/api/chart-of-accounts/${actualTenantId}${queryString}`
    );

    if (!response.ok) {
      throw new Error(`Chart of accounts request failed: ${response.status}`);
    }

    const result = await response.json();
    res.json(result);
  } catch (error) {
    console.error("❌ Chart of accounts API error:", error);
    res.status(500).json({ error: error.message });
  }
});

// Find Unbalanced Transactions endpoint
app.post("/api/find-unbalanced", async (req, res) => {
  try {
    const { organizationName, tenantId, minimumAmount, dateRange } = req.body;

    if (!organizationName && !tenantId) {
      return res
        .status(400)
        .json({ error: "Organization name or tenant ID required" });
    }

    let actualTenantId = tenantId;
    if (organizationName && !tenantId) {
      const connections = await tokenStorage.getAllXeroConnections();
      const connection = connections.find((c) =>
        c.tenantName.toLowerCase().includes(organizationName.toLowerCase())
      );
      if (connection) {
        actualTenantId = connection.tenantId;
      } else {
        return res.status(404).json({ error: "Organization not found" });
      }
    }

    const params = new URLSearchParams();
    if (minimumAmount) params.append("minimumAmount", minimumAmount.toString());
    if (dateRange) params.append("dateRange", dateRange);
    const queryString = params.toString() ? `?${params.toString()}` : "";

    const response = await fetch(
      `${req.protocol}://${req.get(
        "host"
      )}/api/find-unbalanced/${actualTenantId}${queryString}`
    );

    if (!response.ok) {
      throw new Error(`Find unbalanced request failed: ${response.status}`);
    }

    const result = await response.json();
    res.json(result);
  } catch (error) {
    console.error("❌ Find unbalanced API error:", error);
    res.status(500).json({ error: error.message });
  }
});

// Compare Periods endpoint
app.post("/api/compare-periods", async (req, res) => {
  try {
    const { organizationName, tenantId, fromDate, toDate, accountFilter } =
      req.body;

    if (!organizationName && !tenantId) {
      return res
        .status(400)
        .json({ error: "Organization name or tenant ID required" });
    }

    if (!fromDate) {
      return res
        .status(400)
        .json({ error: "fromDate is required for period comparison" });
    }

    let actualTenantId = tenantId;
    if (organizationName && !tenantId) {
      const connections = await tokenStorage.getAllXeroConnections();
      const connection = connections.find((c) =>
        c.tenantName.toLowerCase().includes(organizationName.toLowerCase())
      );
      if (connection) {
        actualTenantId = connection.tenantId;
      } else {
        return res.status(404).json({ error: "Organization not found" });
      }
    }

    const params = new URLSearchParams();
    params.append("fromDate", fromDate);
    if (toDate) params.append("toDate", toDate);
    if (accountFilter) params.append("accountFilter", accountFilter);
    const queryString = "?" + params.toString();

    const response = await fetch(
      `${req.protocol}://${req.get(
        "host"
      )}/api/compare-periods/${actualTenantId}${queryString}`
    );

    if (!response.ok) {
      throw new Error(`Compare periods request failed: ${response.status}`);
    }

    const result = await response.json();
    res.json(result);
  } catch (error) {
    console.error("❌ Compare periods API error:", error);
    res.status(500).json({ error: error.message });
  }
});

// Start auto-refresh when server starts
// Add this to your startServer() function, after initializeDatabase()
async function initializeAutoRefresh() {
  try {
    await initializeDatabase();

    // Start the auto-refresh system
    startAutoRefresh();

    console.error("✅ Auto token refresh system initialized");
  } catch (error) {
    console.error("❌ Failed to initialize auto-refresh:", error);
  }
}

// Add these endpoints to your existing server.js after your current API routes

// ============================================================================
// ENHANCED MCP ANALYSIS ENDPOINTS
// ============================================================================

// Get manual journal entries for analysis
app.get("/api/journal-entries/:tenantId", async (req, res) => {
  try {
    const tokenData = await tokenStorage.getXeroToken(req.params.tenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    await xero.setTokenSet(tokenData);

    // Get date range from query parameters
    const dateFrom =
      req.query.dateFrom ||
      new Date(Date.now() - 365 * 24 * 60 * 60 * 1000)
        .toISOString()
        .split("T")[0]; // 1 year ago
    const dateTo = req.query.dateTo || new Date().toISOString().split("T")[0];
    const accountName = req.query.accountName;

    console.log(
      `Getting journal entries for ${tokenData.tenantName} from ${dateFrom} to ${dateTo}`
    );

    // Get manual journals from Xero
    const response = await xero.accountingApi.getManualJournals(
      req.params.tenantId,
      null, // ifModifiedSince
      `Date >= DateTime(${dateFrom.replace(
        /-/g,
        ","
      )}) AND Date <= DateTime(${dateTo.replace(/-/g, ",")})` // where clause
    );

    const journals = response.body.manualJournals || [];

    // Filter and analyze journals
    const analysisResults = journals
      .map((journal) => {
        const journalLines = journal.journalLines || [];

        // Calculate total debits and credits
        const totalDebits = journalLines
          .filter((line) => line.lineAmount > 0)
          .reduce((sum, line) => sum + line.lineAmount, 0);

        const totalCredits = journalLines
          .filter((line) => line.lineAmount < 0)
          .reduce((sum, line) => sum + Math.abs(line.lineAmount), 0);

        const isBalanced = Math.abs(totalDebits - totalCredits) < 0.01;

        // Check if this journal affects the specified account
        const affectsAccount = accountName
          ? journalLines.some(
              (line) =>
                line.accountCode &&
                line.accountCode
                  .toLowerCase()
                  .includes(accountName.toLowerCase())
            )
          : true;

        if (!affectsAccount) return null;

        return {
          journalID: journal.manualJournalID,
          journalNumber: journal.journalNumber,
          reference: journal.reference,
          date: journal.date,
          status: journal.status,
          totalDebits,
          totalCredits,
          isBalanced,
          imbalanceAmount: totalDebits - totalCredits,
          lineCount: journalLines.length,
          journalLines: journalLines.map((line) => ({
            accountCode: line.accountCode,
            accountName: line.accountName,
            description: line.description,
            lineAmount: line.lineAmount,
            trackingCategories: line.trackingCategories,
          })),
          // Flag suspicious entries
          isSuspicious:
            !isBalanced ||
            Math.abs(totalDebits) > 1000000 || // Large amounts
            journalLines.length === 1 || // Single-sided entries
            journalLines.some(
              (line) =>
                line.accountName &&
                line.accountName.toLowerCase().includes("future fund")
            ),
        };
      })
      .filter((j) => j !== null);

    // Sort by date (newest first)
    analysisResults.sort((a, b) => new Date(b.date) - new Date(a.date));

    console.log(
      `Found ${analysisResults.length} journal entries, ${
        analysisResults.filter((j) => j.isSuspicious).length
      } flagged as suspicious`
    );

    res.json({
      tenantId: req.params.tenantId,
      tenantName: tokenData.tenantName,
      dateFrom,
      dateTo,
      totalJournals: analysisResults.length,
      suspiciousJournals: analysisResults.filter((j) => j.isSuspicious).length,
      unbalancedJournals: analysisResults.filter((j) => !j.isBalanced).length,
      journals: analysisResults,
    });
  } catch (error) {
    console.error("Error getting journal entries:", error);
    res.status(500).json({
      error: "Failed to get journal entries",
      details: error.message,
    });
  }
});

// Analyze equity account movements
app.get("/api/equity-analysis/:tenantId", async (req, res) => {
  try {
    const tokenData = await tokenStorage.getXeroToken(req.params.tenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    await xero.setTokenSet(tokenData);

    const equityAccountName = req.query.equityAccountName || "Future Fund";
    const monthsBack = parseInt(req.query.monthsBack) || 12;

    console.log(
      `Analyzing equity movements for ${equityAccountName} over ${monthsBack} months`
    );

    // Get accounts first to find the equity account ID
    const accountsResponse = await xero.accountingApi.getAccounts(
      req.params.tenantId,
      null,
      `Type=="EQUITY" AND Name.Contains("${equityAccountName}")`
    );

    const equityAccounts = accountsResponse.body.accounts || [];

    if (equityAccounts.length === 0) {
      return res.json({
        error: `No equity account found matching "${equityAccountName}"`,
        tenantName: tokenData.tenantName,
        searchTerm: equityAccountName,
      });
    }

    const results = [];

    for (const account of equityAccounts) {
      // Get account transactions - this requires a different API call
      // Note: Xero's API has limitations on transaction history
      try {
        const today = new Date();
        const startDate = new Date(
          today.getFullYear(),
          today.getMonth() - monthsBack,
          1
        );

        // We'll need to get this data from manual journals since direct account transactions
        // are limited in Xero API
        const journalResponse = await xero.accountingApi.getManualJournals(
          req.params.tenantId,
          null,
          `Date >= DateTime(${startDate.getFullYear()},${
            startDate.getMonth() + 1
          },${startDate.getDate()})`
        );

        const relevantJournals = (journalResponse.body.manualJournals || [])
          .filter(
            (journal) =>
              journal.journalLines &&
              journal.journalLines.some(
                (line) =>
                  line.accountCode === account.code ||
                  (line.accountName &&
                    line.accountName
                      .toLowerCase()
                      .includes(equityAccountName.toLowerCase()))
              )
          )
          .map((journal) => ({
            journalID: journal.manualJournalID,
            journalNumber: journal.journalNumber,
            date: journal.date,
            reference: journal.reference,
            status: journal.status,
            relevantLines: journal.journalLines.filter(
              (line) =>
                line.accountCode === account.code ||
                (line.accountName &&
                  line.accountName
                    .toLowerCase()
                    .includes(equityAccountName.toLowerCase()))
            ),
          }));

        results.push({
          accountID: account.accountID,
          accountCode: account.code,
          accountName: account.name,
          currentBalance: account.currentBalance || 0,
          accountType: account.type,
          status: account.status,
          transactionCount: relevantJournals.length,
          transactions: relevantJournals,
          // Calculate balance changes
          totalMovements: relevantJournals.reduce(
            (sum, j) =>
              sum +
              j.relevantLines.reduce(
                (lineSum, line) => lineSum + line.lineAmount,
                0
              ),
            0
          ),
        });
      } catch (accountError) {
        console.error(`Error analyzing account ${account.name}:`, accountError);
        results.push({
          accountID: account.accountID,
          accountCode: account.code,
          accountName: account.name,
          error: accountError.message,
        });
      }
    }

    res.json({
      tenantId: req.params.tenantId,
      tenantName: tokenData.tenantName,
      analysisDate: new Date().toISOString(),
      searchTerm: equityAccountName,
      monthsAnalyzed: monthsBack,
      accountsFound: results.length,
      accounts: results,
    });
  } catch (error) {
    console.error("Error analyzing equity movements:", error);
    res.status(500).json({
      error: "Failed to analyze equity movements",
      details: error.message,
    });
  }
});

// Get account transaction history
app.get("/api/account-history/:tenantId/:accountName", async (req, res) => {
  try {
    const tokenData = await tokenStorage.getXeroToken(req.params.tenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    await xero.setTokenSet(tokenData);

    const accountName = decodeURIComponent(req.params.accountName);
    const dateFrom = req.query.dateFrom;
    const dateTo = req.query.dateTo;

    console.log(`Getting account history for: ${accountName}`);

    // First, find the account
    const accountsResponse = await xero.accountingApi.getAccounts(
      req.params.tenantId,
      null,
      `Name.Contains("${accountName}")`
    );

    const accounts = accountsResponse.body.accounts || [];
    const matchingAccount = accounts.find(
      (acc) =>
        acc.name.toLowerCase() === accountName.toLowerCase() ||
        acc.name.toLowerCase().includes(accountName.toLowerCase())
    );

    if (!matchingAccount) {
      return res.json({
        error: `Account "${accountName}" not found`,
        tenantName: tokenData.tenantName,
        availableAccounts: accounts.slice(0, 10).map((a) => a.name),
      });
    }

    // Get journals that affect this account
    let whereClause = "";
    if (dateFrom && dateTo) {
      whereClause = `Date >= DateTime(${dateFrom.replace(
        /-/g,
        ","
      )}) AND Date <= DateTime(${dateTo.replace(/-/g, ",")})`;
    }

    const journalResponse = await xero.accountingApi.getManualJournals(
      req.params.tenantId,
      null,
      whereClause
    );

    const relevantJournals = (journalResponse.body.manualJournals || [])
      .filter(
        (journal) =>
          journal.journalLines &&
          journal.journalLines.some(
            (line) =>
              line.accountCode === matchingAccount.code ||
              (line.accountName &&
                line.accountName
                  .toLowerCase()
                  .includes(accountName.toLowerCase()))
          )
      )
      .map((journal) => {
        const relevantLines = journal.journalLines.filter(
          (line) =>
            line.accountCode === matchingAccount.code ||
            (line.accountName &&
              line.accountName
                .toLowerCase()
                .includes(accountName.toLowerCase()))
        );

        return {
          journalID: journal.manualJournalID,
          journalNumber: journal.journalNumber,
          date: journal.date,
          reference: journal.reference,
          status: journal.status,
          description: journal.narration,
          relevantLines: relevantLines,
          netAmount: relevantLines.reduce(
            (sum, line) => sum + line.lineAmount,
            0
          ),
        };
      })
      .sort((a, b) => new Date(b.date) - new Date(a.date));

    res.json({
      tenantId: req.params.tenantId,
      tenantName: tokenData.tenantName,
      account: {
        accountID: matchingAccount.accountID,
        accountCode: matchingAccount.code,
        accountName: matchingAccount.name,
        accountType: matchingAccount.type,
        currentBalance: matchingAccount.currentBalance || 0,
        status: matchingAccount.status,
      },
      dateFrom: dateFrom || "All time",
      dateTo: dateTo || "All time",
      transactionCount: relevantJournals.length,
      transactions: relevantJournals,
      totalMovement: relevantJournals.reduce(
        (sum, t) => sum + Math.abs(t.netAmount),
        0
      ),
    });
  } catch (error) {
    console.error("Error getting account history:", error);
    res.status(500).json({
      error: "Failed to get account history",
      details: error.message,
    });
  }
});

// Find unbalanced transactions
app.get("/api/find-unbalanced/:tenantId", async (req, res) => {
  try {
    const tokenData = await tokenStorage.getXeroToken(req.params.tenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    await xero.setTokenSet(tokenData);

    const minimumAmount = parseFloat(req.query.minimumAmount) || 10000;
    const dateRange = req.query.dateRange || "1year";

    // Calculate date range
    const today = new Date();
    let startDate = new Date();

    switch (dateRange) {
      case "3months":
        startDate.setMonth(today.getMonth() - 3);
        break;
      case "1year":
        startDate.setFullYear(today.getFullYear() - 1);
        break;
      case "all":
        startDate = new Date("2000-01-01");
        break;
      default:
        startDate.setFullYear(today.getFullYear() - 1);
    }

    console.log(
      `Finding unbalanced transactions >= $${minimumAmount} since ${
        startDate.toISOString().split("T")[0]
      }`
    );

    const whereClause = `Date >= DateTime(${startDate.getFullYear()},${
      startDate.getMonth() + 1
    },${startDate.getDate()})`;

    const journalResponse = await xero.accountingApi.getManualJournals(
      req.params.tenantId,
      null,
      whereClause
    );

    const journals = journalResponse.body.manualJournals || [];

    const unbalancedTransactions = journals
      .map((journal) => {
        const journalLines = journal.journalLines || [];

        const totalDebits = journalLines
          .filter((line) => line.lineAmount > 0)
          .reduce((sum, line) => sum + line.lineAmount, 0);

        const totalCredits = journalLines
          .filter((line) => line.lineAmount < 0)
          .reduce((sum, line) => sum + Math.abs(line.lineAmount), 0);

        const imbalance = totalDebits - totalCredits;
        const isUnbalanced = Math.abs(imbalance) >= minimumAmount;
        const hasLargeAmount =
          Math.max(totalDebits, totalCredits) >= minimumAmount;

        if (!isUnbalanced && !hasLargeAmount) return null;

        return {
          journalID: journal.manualJournalID,
          journalNumber: journal.journalNumber,
          reference: journal.reference,
          date: journal.date,
          status: journal.status,
          totalDebits,
          totalCredits,
          imbalanceAmount: imbalance,
          isUnbalanced,
          severity:
            Math.abs(imbalance) > 1000000
              ? "CRITICAL"
              : Math.abs(imbalance) > 100000
              ? "HIGH"
              : "MEDIUM",
          journalLines: journalLines.map((line) => ({
            accountCode: line.accountCode,
            accountName: line.accountName,
            description: line.description,
            lineAmount: line.lineAmount,
          })),
          flags: {
            largeAmount: Math.max(totalDebits, totalCredits) > 1000000,
            unbalanced: isUnbalanced,
            singleSided: journalLines.length === 1,
            affectsFutureFund: journalLines.some(
              (line) =>
                line.accountName &&
                line.accountName.toLowerCase().includes("future fund")
            ),
          },
        };
      })
      .filter((j) => j !== null)
      .sort(
        (a, b) => Math.abs(b.imbalanceAmount) - Math.abs(a.imbalanceAmount)
      );

    res.json({
      tenantId: req.params.tenantId,
      tenantName: tokenData.tenantName,
      analysisDate: new Date().toISOString(),
      criteria: {
        minimumAmount,
        dateRange,
        startDate: startDate.toISOString().split("T")[0],
      },
      summary: {
        totalJournalsAnalyzed: journals.length,
        unbalancedFound: unbalancedTransactions.filter((t) => t.isUnbalanced)
          .length,
        largeAmountFound: unbalancedTransactions.filter(
          (t) => t.flags.largeAmount
        ).length,
        criticalIssues: unbalancedTransactions.filter(
          (t) => t.severity === "CRITICAL"
        ).length,
        futureFundRelated: unbalancedTransactions.filter(
          (t) => t.flags.affectsFutureFund
        ).length,
      },
      transactions: unbalancedTransactions,
    });
  } catch (error) {
    console.error("Error finding unbalanced transactions:", error);
    res.status(500).json({
      error: "Failed to find unbalanced transactions",
      details: error.message,
    });
  }
});

// Get complete chart of accounts
app.get("/api/chart-of-accounts/:tenantId", async (req, res) => {
  try {
    const tokenData = await tokenStorage.getXeroToken(req.params.tenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    await xero.setTokenSet(tokenData);

    const accountType = req.query.accountType;
    const includeArchived = req.query.includeArchived === "true";

    let whereClause = "";
    if (accountType) {
      whereClause = `Type=="${accountType}"`;
    }
    if (!includeArchived) {
      whereClause += whereClause ? ' AND Status=="ACTIVE"' : 'Status=="ACTIVE"';
    }

    console.log(`Getting chart of accounts for ${tokenData.tenantName}`);

    const response = await xero.accountingApi.getAccounts(
      req.params.tenantId,
      null,
      whereClause
    );

    const accounts = response.body.accounts || [];

    // Analyze accounts for unusual patterns
    const analysis = accounts.map((account) => {
      const balance = parseFloat(account.currentBalance) || 0;
      const isLargeBalance = Math.abs(balance) > 1000000;
      const isUnusualEquity =
        account.type === "EQUITY" &&
        (account.name.toLowerCase().includes("future fund") ||
          account.name.toLowerCase().includes("reserve") ||
          Math.abs(balance) > 10000000);

      return {
        accountID: account.accountID,
        code: account.code,
        name: account.name,
        type: account.type,
        class: account.class,
        status: account.status,
        currentBalance: balance,
        description: account.description,
        systemAccount: account.systemAccount,
        flags: {
          largeBalance: isLargeBalance,
          unusualEquity: isUnusualEquity,
          zeroBalance: balance === 0,
          negativeAsset: account.type === "ASSET" && balance < 0,
          positiveExpense: account.type === "EXPENSE" && balance > 0,
        },
      };
    });

    // Group by account type
    const groupedAccounts = {
      ASSET: analysis.filter((a) => a.type === "ASSET"),
      LIABILITY: analysis.filter((a) => a.type === "LIABILITY"),
      EQUITY: analysis.filter((a) => a.type === "EQUITY"),
      REVENUE: analysis.filter((a) => a.type === "REVENUE"),
      EXPENSE: analysis.filter((a) => a.type === "EXPENSE"),
      OTHER: analysis.filter(
        (a) =>
          !["ASSET", "LIABILITY", "EQUITY", "REVENUE", "EXPENSE"].includes(
            a.type
          )
      ),
    };

    res.json({
      tenantId: req.params.tenantId,
      tenantName: tokenData.tenantName,
      filters: {
        accountType: accountType || "All",
        includeArchived,
      },
      summary: {
        totalAccounts: accounts.length,
        activeAccounts: analysis.filter((a) => a.status === "ACTIVE").length,
        archivedAccounts: analysis.filter((a) => a.status === "ARCHIVED")
          .length,
        largeBalanceAccounts: analysis.filter((a) => a.flags.largeBalance)
          .length,
        unusualEquityAccounts: analysis.filter((a) => a.flags.unusualEquity)
          .length,
        accountsByType: {
          ASSET: groupedAccounts.ASSET.length,
          LIABILITY: groupedAccounts.LIABILITY.length,
          EQUITY: groupedAccounts.EQUITY.length,
          REVENUE: groupedAccounts.REVENUE.length,
          EXPENSE: groupedAccounts.EXPENSE.length,
          OTHER: groupedAccounts.OTHER.length,
        },
      },
      accounts: groupedAccounts,
      flaggedAccounts: analysis.filter(
        (a) =>
          a.flags.largeBalance ||
          a.flags.unusualEquity ||
          a.flags.negativeAsset ||
          a.flags.positiveExpense
      ),
    });
  } catch (error) {
    console.error("Error getting chart of accounts:", error);
    res.status(500).json({
      error: "Failed to get chart of accounts",
      details: error.message,
    });
  }
});

// Compare trial balance between periods
app.get("/api/compare-periods/:tenantId", async (req, res) => {
  try {
    const tokenData = await tokenStorage.getXeroToken(req.params.tenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    const fromDate = req.query.fromDate;
    const toDate = req.query.toDate || new Date().toISOString().split("T")[0];
    const accountFilter = req.query.accountFilter;

    if (!fromDate) {
      return res.status(400).json({ error: "fromDate parameter is required" });
    }

    console.log(
      `Comparing periods: ${fromDate} vs ${toDate} for ${tokenData.tenantName}`
    );

    // Get trial balance for both periods by calling our existing endpoint
    const [fromPeriodResponse, toPeriodResponse] = await Promise.all([
      fetch(
        `${req.protocol}://${req.get("host")}/api/trial-balance/${
          req.params.tenantId
        }?date=${fromDate}`
      ),
      fetch(
        `${req.protocol}://${req.get("host")}/api/trial-balance/${
          req.params.tenantId
        }?date=${toDate}`
      ),
    ]);

    if (!fromPeriodResponse.ok || !toPeriodResponse.ok) {
      throw new Error("Failed to retrieve trial balance data for comparison");
    }

    const fromPeriodData = await fromPeriodResponse.json();
    const toPeriodData = await toPeriodResponse.json();

    // Compare the periods
    const comparison = {
      tenantId: req.params.tenantId,
      tenantName: tokenData.tenantName,
      fromDate,
      toDate,
      fromPeriod: {
        totalAssets: fromPeriodData.trialBalance.totals.totalAssets,
        totalLiabilities: fromPeriodData.trialBalance.totals.totalLiabilities,
        totalEquity: fromPeriodData.trialBalance.totals.totalEquity,
        totalDebits: fromPeriodData.trialBalance.totals.totalDebits,
        totalCredits: fromPeriodData.trialBalance.totals.totalCredits,
        balanced: fromPeriodData.balanceCheck.debitsEqualCredits,
      },
      toPeriod: {
        totalAssets: toPeriodData.trialBalance.totals.totalAssets,
        totalLiabilities: toPeriodData.trialBalance.totals.totalLiabilities,
        totalEquity: toPeriodData.trialBalance.totals.totalEquity,
        totalDebits: toPeriodData.trialBalance.totals.totalDebits,
        totalCredits: toPeriodData.trialBalance.totals.totalCredits,
        balanced: toPeriodData.balanceCheck.debitsEqualCredits,
      },
      changes: {
        assetsChange:
          toPeriodData.trialBalance.totals.totalAssets -
          fromPeriodData.trialBalance.totals.totalAssets,
        liabilitiesChange:
          toPeriodData.trialBalance.totals.totalLiabilities -
          fromPeriodData.trialBalance.totals.totalLiabilities,
        equityChange:
          toPeriodData.trialBalance.totals.totalEquity -
          fromPeriodData.trialBalance.totals.totalEquity,
        balanceStatusChange:
          toPeriodData.balanceCheck.debitsEqualCredits !==
          fromPeriodData.balanceCheck.debitsEqualCredits,
      },
    };

    // Find accounts with significant changes
    const fromAccounts = [
      ...fromPeriodData.trialBalance.assets,
      ...fromPeriodData.trialBalance.liabilities,
      ...fromPeriodData.trialBalance.equity,
    ];
    const toAccounts = [
      ...toPeriodData.trialBalance.assets,
      ...toPeriodData.trialBalance.liabilities,
      ...toPeriodData.trialBalance.equity,
    ];

    const accountChanges = [];

    // Find changes in existing accounts
    fromAccounts.forEach((fromAcc) => {
      const toAcc = toAccounts.find((a) => a.name === fromAcc.name);
      if (toAcc) {
        const change = toAcc.balance - fromAcc.balance;
        if (Math.abs(change) > 1000) {
          // Only show changes > $1,000
          accountChanges.push({
            accountName: fromAcc.name,
            fromBalance: fromAcc.balance,
            toBalance: toAcc.balance,
            change: change,
            changeType: change > 0 ? "INCREASE" : "DECREASE",
          });
        }
      }
    });

    // Find new accounts
    toAccounts.forEach((toAcc) => {
      const fromAcc = fromAccounts.find((a) => a.name === toAcc.name);
      if (!fromAcc && Math.abs(toAcc.balance) > 1000) {
        accountChanges.push({
          accountName: toAcc.name,
          fromBalance: 0,
          toBalance: toAcc.balance,
          change: toAcc.balance,
          changeType: "NEW_ACCOUNT",
        });
      }
    });

    // Sort by magnitude of change
    accountChanges.sort((a, b) => Math.abs(b.change) - Math.abs(a.change));

    comparison.accountChanges = accountChanges;
    comparison.significantChanges = accountChanges.filter(
      (c) => Math.abs(c.change) > 100000
    );

    res.json(comparison);
  } catch (error) {
    console.error("Error comparing periods:", error);
    res.status(500).json({
      error: "Failed to compare periods",
      details: error.message,
    });
  }
});

// Update your existing startServer function to call initializeAutoRefresh()
// Replace: await initializeDatabase();
// With: await initializeAutoRefresh();

// Health check endpoint
app.get("/api/health", async (req, res) => {
  try {
    // Test database connection
    const dbTest = await pool.query("SELECT NOW()");
    const xeroConnections = await tokenStorage.getAllXeroConnections();

    res.json({
      status: "healthy",
      timestamp: new Date().toISOString(),
      database: "connected",
      xeroConnections: xeroConnections.length,
      uptime: process.uptime(),
    });
  } catch (error) {
    res.status(500).json({
      status: "unhealthy",
      error: error.message,
      timestamp: new Date().toISOString(),
    });
  }
});

// DATABASE DEBUG endpoint - Add this to see what's stored
app.get("/api/debug/database", async (req, res) => {
  try {
    console.error("🔍 DEBUG: Checking database contents...");

    // Get all tokens from database
    const result = await pool.query(
      "SELECT tenant_id, tenant_name, provider, expires_at, last_seen FROM tokens ORDER BY last_seen DESC"
    );

    const now = Date.now();
    const tokens = result.rows.map((row) => ({
      tenant_id: row.tenant_id,
      tenant_name: row.tenant_name,
      provider: row.provider,
      expired: now > row.expires_at,
      expires_in_minutes: Math.floor((row.expires_at - now) / (1000 * 60)),
      last_seen: row.last_seen,
    }));

    console.error("✅ DEBUG: Database tokens:", tokens);

    res.json({
      totalTokens: tokens.length,
      tokens: tokens,
      currentTime: new Date().toISOString(),
      currentTimestamp: now,
    });
  } catch (error) {
    console.error("❌ DEBUG: Database error:", error);
    res.status(500).json({
      error: "Database query failed",
      details: error.message,
    });
  }
});

// Serve login manager as default
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "login-manager.html"));
});

// Serve main dashboard
app.get("/dashboard", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// ==============================================================================
// YoY Analyst EndPoint
// ==============================================================================
// Year-over-Year Analysis endpoint - UPDATED TO USE 24 MONTHLY REPORTS
app.get("/api/yoy-analysis/:tenantId", async (req, res) => {
  try {
    const tokenData = await tokenStorage.getXeroToken(req.params.tenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    const reportDate = req.query.date || new Date().toISOString().split("T")[0];

    await xero.setTokenSet(tokenData);

    // Generate 24 monthly periods (current 12 months + previous 12 months)
    const reportMonth = new Date(reportDate);
    const currentYearPeriods = [];
    const previousYearPeriods = [];

    // Current year - 12 months ending with report month
    for (let i = 11; i >= 0; i--) {
      const monthDate = new Date(reportMonth);
      monthDate.setMonth(reportMonth.getMonth() - i);

      const monthStart = new Date(
        monthDate.getFullYear(),
        monthDate.getMonth(),
        1
      );
      const monthEnd = new Date(
        monthDate.getFullYear(),
        monthDate.getMonth() + 1,
        0
      );

      currentYearPeriods.push({
        label: monthStart.toLocaleDateString("en-US", {
          month: "short",
          year: "numeric",
        }),
        startDate: monthStart.toISOString().split("T")[0],
        endDate: monthEnd.toISOString().split("T")[0],
      });
    }

    // Previous year - same 12 months but one year earlier
    for (let i = 11; i >= 0; i--) {
      const monthDate = new Date(reportMonth);
      monthDate.setFullYear(reportMonth.getFullYear() - 1);
      monthDate.setMonth(reportMonth.getMonth() - i);

      const monthStart = new Date(
        monthDate.getFullYear(),
        monthDate.getMonth(),
        1
      );
      const monthEnd = new Date(
        monthDate.getFullYear(),
        monthDate.getMonth() + 1,
        0
      );

      previousYearPeriods.push({
        label: monthStart.toLocaleDateString("en-US", {
          month: "short",
          year: "numeric",
        }),
        startDate: monthStart.toISOString().split("T")[0],
        endDate: monthEnd.toISOString().split("T")[0],
      });
    }

    // Function to get monthly P&L data
    async function getMonthlyPLData(periods, periodLabel) {
      let totalRevenue = 0;
      let totalExpenses = 0;
      const monthlyDetails = [];

      for (const period of periods) {
        try {
          const response = await xero.accountingApi.getReportProfitAndLoss(
            req.params.tenantId,
            period.startDate,
            period.endDate
          );

          const plRows = response.body.reports?.[0]?.rows || [];
          const monthlyPL = parsePLData(plRows);

          monthlyDetails.push({
            ...period,
            revenue: monthlyPL.totalRevenue,
            expenses: monthlyPL.totalExpenses,
            profit: monthlyPL.totalRevenue - monthlyPL.totalExpenses,
          });

          totalRevenue += monthlyPL.totalRevenue;
          totalExpenses += monthlyPL.totalExpenses;
        } catch (monthError) {
          console.error(
            `Error loading ${periodLabel} ${period.label}:`,
            monthError.message
          );
          monthlyDetails.push({
            ...period,
            revenue: 0,
            expenses: 0,
            profit: 0,
            error: monthError.message,
          });
        }
      }

      return {
        totalRevenue,
        totalExpenses,
        totalProfit: totalRevenue - totalExpenses,
        monthlyDetails,
      };
    }

    // Get both periods in parallel
    const [currentYearData, previousYearData] = await Promise.all([
      getMonthlyPLData(currentYearPeriods, "Current"),
      getMonthlyPLData(previousYearPeriods, "Previous"),
    ]);

    // Get trial balance data for asset/equity information
    const [currentTBResponse, previousTBResponse] = await Promise.all([
      fetch(
        `${req.protocol}://${req.get("host")}/api/trial-balance/${
          req.params.tenantId
        }?date=${reportDate}`
      ),
      fetch(
        `${req.protocol}://${req.get("host")}/api/trial-balance/${
          req.params.tenantId
        }?date=${previousYearPeriods[11].endDate}`
      ),
    ]);

    let currentTB = null,
      previousTB = null;
    if (currentTBResponse.ok) {
      currentTB = await currentTBResponse.json();
    }
    if (previousTBResponse.ok) {
      previousTB = await previousTBResponse.json();
    }

    // Calculate YoY metrics using monthly totals
    const yoyAnalysis = {
      periods: {
        current: {
          label: `${currentYearPeriods[0].label.split(" ")[1]}-${
            currentYearPeriods[11].label
          }`,
          start: currentYearPeriods[0].startDate,
          end: currentYearPeriods[11].endDate,
          revenue: currentYearData.totalRevenue,
          expenses: currentYearData.totalExpenses,
          profit: currentYearData.totalProfit,
          assets: currentTB?.trialBalance?.totals?.totalAssets || 0,
          equity: currentTB?.trialBalance?.totals?.totalEquity || 0,
          monthlyBreakdown: currentYearData.monthlyDetails,
        },
        previous: {
          label: `${previousYearPeriods[0].label.split(" ")[1]}-${
            previousYearPeriods[11].label
          }`,
          start: previousYearPeriods[0].startDate,
          end: previousYearPeriods[11].endDate,
          revenue: previousYearData.totalRevenue,
          expenses: previousYearData.totalExpenses,
          profit: previousYearData.totalProfit,
          assets: previousTB?.trialBalance?.totals?.totalAssets || 0,
          equity: previousTB?.trialBalance?.totals?.totalEquity || 0,
          monthlyBreakdown: previousYearData.monthlyDetails,
        },
      },
      growth: {
        revenue: calculateGrowthRate(
          previousYearData.totalRevenue,
          currentYearData.totalRevenue
        ),
        profit: calculateGrowthRate(
          previousYearData.totalProfit,
          currentYearData.totalProfit
        ),
        assets: calculateGrowthRate(
          previousTB?.trialBalance?.totals?.totalAssets || 0,
          currentTB?.trialBalance?.totals?.totalAssets || 0
        ),
        equity: calculateGrowthRate(
          previousTB?.trialBalance?.totals?.totalEquity || 0,
          currentTB?.trialBalance?.totals?.totalEquity || 0
        ),
      },
      margins: {
        current: calculateMargin(
          currentYearData.totalRevenue,
          currentYearData.totalProfit
        ),
        previous: calculateMargin(
          previousYearData.totalRevenue,
          previousYearData.totalProfit
        ),
      },
    };

    res.json({
      tenantId: req.params.tenantId,
      tenantName: tokenData.tenantName,
      analysis: yoyAnalysis,
      dataSource: "24 monthly P&L reports",
      generatedAt: new Date().toISOString(),
    });
  } catch (error) {
    console.error("Error getting YoY analysis with monthly reports:", error);
    res.status(500).json({
      error: "Failed to get YoY analysis",
      details: error.message,
    });
  }
});

// Helper function to parse P&L data - reduced logs and improved
function parsePLData(plRows) {
  const plData = {
    totalRevenue: 0,
    totalExpenses: 0,
    revenueAccounts: [],
    expenseAccounts: [],
  };

  plRows.forEach((section) => {
    if (section.rowType === "Section" && section.rows && section.title) {
      const sectionTitle = section.title.toLowerCase();

      section.rows.forEach((row) => {
        if (row.rowType === "Row" && row.cells && row.cells.length >= 2) {
          const accountName = row.cells[0]?.value || "";
          const amount = parseFloat(row.cells[1]?.value || 0);

          if (accountName.toLowerCase().includes("total") || amount === 0) {
            return;
          }

          if (
            sectionTitle.includes("income") ||
            sectionTitle.includes("revenue") ||
            sectionTitle.includes("trading")
          ) {
            plData.revenueAccounts.push({ name: accountName, amount });
            plData.totalRevenue += amount; // Remove Math.abs()
          } else if (
            sectionTitle.includes("expense") ||
            sectionTitle.includes("cost") ||
            sectionTitle.includes("administration") ||
            sectionTitle.includes("operating")
          ) {
            plData.expenseAccounts.push({ name: accountName, amount });
            plData.totalExpenses += amount; // Remove Math.abs()
          }
        }
      });
    }
  });

  return plData;
}

// Helper function to calculate growth rate
function calculateGrowthRate(previousValue, currentValue) {
  if (previousValue === 0) {
    return currentValue > 0 ? 100 : 0;
  }
  return ((currentValue - previousValue) / Math.abs(previousValue)) * 100;
}

// Helper function to calculate profit margin
function calculateMargin(revenue, profit) {
  return revenue > 0 ? (profit / revenue) * 100 : 0;
}

// POST wrapper for YoY Analysis
app.post("/api/yoy-analysis", async (req, res) => {
  try {
    const { organizationName, tenantId, date } = req.body;

    if (!organizationName && !tenantId) {
      return res
        .status(400)
        .json({ error: "Organization name or tenant ID required" });
    }

    let actualTenantId = tenantId;
    if (organizationName && !tenantId) {
      const connections = await tokenStorage.getAllXeroConnections();
      const connection = connections.find((c) =>
        c.tenantName.toLowerCase().includes(organizationName.toLowerCase())
      );
      if (connection) {
        actualTenantId = connection.tenantId;
      } else {
        return res.status(404).json({ error: "Organization not found" });
      }
    }

    const dateParam = date ? `?date=${date}` : "";
    const response = await fetch(
      `${req.protocol}://${req.get(
        "host"
      )}/api/yoy-analysis/${actualTenantId}${dateParam}`
    );

    if (!response.ok) {
      throw new Error(`YoY analysis request failed: ${response.status}`);
    }

    const result = await response.json();
    res.json(result);
  } catch (error) {
    console.error("YoY analysis API error:", error);
    res.status(500).json({ error: error.message });
  }
});

// ==============================================================================
// ENHANCED TRIAL BALANCE ENDPOINTS WITH DATE SUPPORT
// ==============================================================================

// Enhanced Individual Trial Balance with Date Support
app.get("/api/trial-balance/:tenantId", async (req, res) => {
  try {
    console.log(
      `🔍 Getting PROPER trial balance for tenant: ${req.params.tenantId}`
    );

    const tokenData = await tokenStorage.getXeroToken(req.params.tenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    await xero.setTokenSet(tokenData);

    // Get date from query parameter or use today
    const reportDate = req.query.date || new Date().toISOString().split("T")[0];
    console.log(`📅 Report date: ${reportDate}`);

    // Get Balance Sheet report for specified date
    const balanceSheetResponse = await xero.accountingApi.getReportBalanceSheet(
      req.params.tenantId,
      reportDate
    );

    const balanceSheetRows = balanceSheetResponse.body.reports?.[0]?.rows || [];

    // Initialize trial balance structure
    const trialBalance = {
      assets: [],
      liabilities: [],
      equity: [],
      revenue: [],
      expenses: [],
      totals: {
        totalDebits: 0,
        totalCredits: 0,
        totalAssets: 0,
        totalLiabilities: 0,
        totalEquity: 0,
        totalRevenue: 0,
        totalExpenses: 0,
      },
    };

    let processedAccounts = 0;

    // Process each Balance Sheet section
    balanceSheetRows.forEach((section, sectionIndex) => {
      if (section.rowType === "Section" && section.rows && section.title) {
        const sectionTitle = section.title.toLowerCase();

        section.rows.forEach((row) => {
          if (row.rowType === "Row" && row.cells && row.cells.length >= 2) {
            const accountName = row.cells[0]?.value || "";
            const currentBalance = parseFloat(row.cells[1]?.value || 0);

            if (
              accountName.toLowerCase().includes("total") ||
              currentBalance === 0
            ) {
              return;
            }

            processedAccounts++;

            const accountInfo = {
              name: accountName,
              balance: currentBalance,
              debit: 0,
              credit: 0,
              section: section.title,
            };

            // Account classification logic
            if (
              sectionTitle.includes("bank") ||
              sectionTitle.includes("asset")
            ) {
              accountInfo.debit = currentBalance >= 0 ? currentBalance : 0;
              accountInfo.credit =
                currentBalance < 0 ? Math.abs(currentBalance) : 0;
              trialBalance.assets.push(accountInfo);
              trialBalance.totals.totalAssets += currentBalance;
            } else if (sectionTitle.includes("liabilit")) {
              accountInfo.credit = currentBalance >= 0 ? currentBalance : 0;
              accountInfo.debit =
                currentBalance < 0 ? Math.abs(currentBalance) : 0;
              trialBalance.liabilities.push(accountInfo);
              trialBalance.totals.totalLiabilities += currentBalance;
            } else if (sectionTitle.includes("equity")) {
              accountInfo.credit = currentBalance >= 0 ? currentBalance : 0;
              accountInfo.debit =
                currentBalance < 0 ? Math.abs(currentBalance) : 0;
              trialBalance.equity.push(accountInfo);
              trialBalance.totals.totalEquity += currentBalance;
            }

            trialBalance.totals.totalDebits += accountInfo.debit;
            trialBalance.totals.totalCredits += accountInfo.credit;
          }
        });
      }
    });

    // Get P&L data for the same date
    try {
      console.error("🔄 Fetching P&L report for Revenue/Expenses...");
      const profitLossResponse =
        await xero.accountingApi.getReportProfitAndLoss(
          req.params.tenantId,
          reportDate,
          reportDate
        );

      const plRows = profitLossResponse.body.reports?.[0]?.rows || [];

      plRows.forEach((section) => {
        if (section.rowType === "Section" && section.rows && section.title) {
          const sectionTitle = section.title.toLowerCase();

          section.rows.forEach((row) => {
            if (row.rowType === "Row" && row.cells && row.cells.length >= 2) {
              const accountName = row.cells[0]?.value || "";
              const currentAmount = parseFloat(row.cells[1]?.value || 0);

              if (
                accountName.toLowerCase().includes("total") ||
                currentAmount === 0
              ) {
                return;
              }

              processedAccounts++;
              const accountInfo = {
                name: accountName,
                balance: currentAmount,
                debit: 0,
                credit: 0,
                section: section.title,
              };

              if (
                sectionTitle.includes("income") ||
                sectionTitle.includes("revenue") ||
                sectionTitle.includes("trading") ||
                sectionTitle.includes("sales") ||
                sectionTitle.includes("property maintenance")
              ) {
                accountInfo.credit = Math.abs(currentAmount);
                trialBalance.revenue.push(accountInfo);
                trialBalance.totals.totalRevenue += Math.abs(currentAmount);
              } else if (
                sectionTitle.includes("expense") ||
                sectionTitle.includes("cost") ||
                sectionTitle.includes("administration") ||
                sectionTitle.includes("operating")
              ) {
                accountInfo.debit = Math.abs(currentAmount);
                trialBalance.expenses.push(accountInfo);
                trialBalance.totals.totalExpenses += Math.abs(currentAmount);
              }

              trialBalance.totals.totalDebits += accountInfo.debit;
              trialBalance.totals.totalCredits += accountInfo.credit;
            }
          });
        }
      });
    } catch (plError) {
      console.errorlog("⚠️ Could not fetch P&L data:", plError.message);
    }

    // Sort and calculate balance check
    ["assets", "liabilities", "equity", "revenue", "expenses"].forEach(
      (category) => {
        trialBalance[category].sort((a, b) => a.name.localeCompare(b.name));
      }
    );

    const balanceCheck = {
      debitsEqualCredits:
        Math.abs(
          trialBalance.totals.totalDebits - trialBalance.totals.totalCredits
        ) < 0.01,
      difference:
        trialBalance.totals.totalDebits - trialBalance.totals.totalCredits,
      accountingEquation: {
        assets: trialBalance.totals.totalAssets,
        liabilitiesAndEquity:
          trialBalance.totals.totalLiabilities +
          trialBalance.totals.totalEquity,
        balanced:
          Math.abs(
            trialBalance.totals.totalAssets -
              (trialBalance.totals.totalLiabilities +
                trialBalance.totals.totalEquity)
          ) < 0.01,
      },
    };

    console.log(
      `✅ PROPER Trial balance completed for ${tokenData.tenantName} as at ${reportDate}:`,
      {
        processedAccounts: processedAccounts,
        totalAssets: trialBalance.totals.totalAssets,
        totalLiabilities: trialBalance.totals.totalLiabilities,
        totalEquity: trialBalance.totals.totalEquity,
        balanced: balanceCheck.debitsEqualCredits,
      }
    );

    res.json({
      tenantId: req.params.tenantId,
      tenantName: tokenData.tenantName,
      trialBalance,
      balanceCheck,
      generatedAt: new Date().toISOString(),
      reportDate: reportDate,
      processedAccounts: processedAccounts,
      dataSource: "Balance Sheet + P&L Reports",
    });
  } catch (error) {
    console.error("❌ Error getting PROPER trial balance:", error);
    res.status(500).json({
      error: "Failed to get trial balance",
      details: error.message,
      tenantId: req.params.tenantId,
    });
  }
});

// Enhanced Consolidated Trial Balance with Date Support
app.get("/api/consolidated-trial-balance", async (req, res) => {
  try {
    // Get date from query parameter or use today
    const reportDate = req.query.date || new Date().toISOString().split("T")[0];
    console.log(
      `🔄 Loading HIERARCHICAL consolidated trial balance for ${reportDate}...`
    );

    const xeroConnections = await tokenStorage.getAllXeroConnections();
    const connectedXeroEntities = xeroConnections.filter(
      (conn) => conn.connected
    );

    console.log(`🏢 Found ${connectedXeroEntities.length} connected entities`);

    const hierarchicalTrialBalance = {
      consolidated: {
        totals: {
          totalDebits: 0,
          totalCredits: 0,
          totalAssets: 0,
          totalLiabilities: 0,
          totalEquity: 0,
          totalRevenue: 0,
          totalExpenses: 0,
        },
        balanceCheck: {
          debitsEqualCredits: false,
          difference: 0,
          accountingEquation: {
            assets: 0,
            liabilitiesAndEquity: 0,
            balanced: false,
          },
        },
      },
      companies: [],
      reportDate: reportDate,
      generatedAt: new Date().toISOString(),
    };

    // Process each entity with the specified date
    for (const connection of connectedXeroEntities) {
      try {
        console.log(
          `🔄 Processing entity: ${connection.tenantName} for ${reportDate}`
        );

        const trialBalanceResponse = await fetch(
          `${req.protocol}://${req.get("host")}/api/trial-balance/${
            connection.tenantId
          }?date=${reportDate}`
        );

        if (trialBalanceResponse.ok) {
          const entityTrialBalance = await trialBalanceResponse.json();

          // Create hierarchical company structure
          const companyData = {
            tenantId: connection.tenantId,
            tenantName: connection.tenantName,
            balanceCheck: entityTrialBalance.balanceCheck,
            totals: entityTrialBalance.trialBalance.totals,
            reportDate: entityTrialBalance.reportDate,
            sections: {
              assets: {
                title: "Assets",
                total: entityTrialBalance.trialBalance.totals.totalAssets,
                accounts: entityTrialBalance.trialBalance.assets.map(
                  (account) => ({
                    name: account.name,
                    debit: account.debit,
                    credit: account.credit,
                    balance: account.balance,
                    section: account.section || "Assets",
                  })
                ),
              },
              liabilities: {
                title: "Liabilities",
                total: entityTrialBalance.trialBalance.totals.totalLiabilities,
                accounts: entityTrialBalance.trialBalance.liabilities.map(
                  (account) => ({
                    name: account.name,
                    debit: account.debit,
                    credit: account.credit,
                    balance: account.balance,
                    section: account.section || "Liabilities",
                  })
                ),
              },
              equity: {
                title: "Equity",
                total: entityTrialBalance.trialBalance.totals.totalEquity,
                accounts: entityTrialBalance.trialBalance.equity.map(
                  (account) => ({
                    name: account.name,
                    debit: account.debit,
                    credit: account.credit,
                    balance: account.balance,
                    section: account.section || "Equity",
                  })
                ),
              },
              revenue: {
                title: "Revenue",
                total: entityTrialBalance.trialBalance.totals.totalRevenue,
                accounts: entityTrialBalance.trialBalance.revenue.map(
                  (account) => ({
                    name: account.name,
                    debit: account.debit,
                    credit: account.credit,
                    balance: account.balance,
                    section: account.section || "Revenue",
                  })
                ),
              },
              expenses: {
                title: "Expenses",
                total: entityTrialBalance.trialBalance.totals.totalExpenses,
                accounts: entityTrialBalance.trialBalance.expenses.map(
                  (account) => ({
                    name: account.name,
                    debit: account.debit,
                    credit: account.credit,
                    balance: account.balance,
                    section: account.section || "Expenses",
                  })
                ),
              },
            },
            accountCounts: {
              totalAccounts: Object.values({
                assets: entityTrialBalance.trialBalance.assets,
                liabilities: entityTrialBalance.trialBalance.liabilities,
                equity: entityTrialBalance.trialBalance.equity,
                revenue: entityTrialBalance.trialBalance.revenue,
                expenses: entityTrialBalance.trialBalance.expenses,
              }).reduce((sum, accounts) => sum + accounts.length, 0),
              assetAccounts: entityTrialBalance.trialBalance.assets.length,
              liabilityAccounts:
                entityTrialBalance.trialBalance.liabilities.length,
              equityAccounts: entityTrialBalance.trialBalance.equity.length,
              revenueAccounts: entityTrialBalance.trialBalance.revenue.length,
              expenseAccounts: entityTrialBalance.trialBalance.expenses.length,
            },
          };

          hierarchicalTrialBalance.companies.push(companyData);

          // Add to consolidated totals
          const totals = hierarchicalTrialBalance.consolidated.totals;
          const entityTotals = entityTrialBalance.trialBalance.totals;

          totals.totalDebits += entityTotals.totalDebits;
          totals.totalCredits += entityTotals.totalCredits;
          totals.totalAssets += entityTotals.totalAssets;
          totals.totalLiabilities += entityTotals.totalLiabilities;
          totals.totalEquity += entityTotals.totalEquity;
          totals.totalRevenue += entityTotals.totalRevenue;
          totals.totalExpenses += entityTotals.totalExpenses;

          console.log(
            `✅ Added ${connection.tenantName} to hierarchical structure for ${reportDate}`
          );
        }
      } catch (error) {
        console.error(
          `❌ Error loading trial balance for ${connection.tenantId}:`,
          error
        );
      }
    }

    // Calculate consolidated balance check and summary
    const totals = hierarchicalTrialBalance.consolidated.totals;
    hierarchicalTrialBalance.consolidated.balanceCheck = {
      debitsEqualCredits:
        Math.abs(totals.totalDebits - totals.totalCredits) < 0.01,
      difference: totals.totalDebits - totals.totalCredits,
      accountingEquation: {
        assets: totals.totalAssets,
        liabilitiesAndEquity: totals.totalLiabilities + totals.totalEquity,
        balanced:
          Math.abs(
            totals.totalAssets - (totals.totalLiabilities + totals.totalEquity)
          ) < 0.01,
      },
    };

    hierarchicalTrialBalance.summary = {
      totalCompanies: hierarchicalTrialBalance.companies.length,
      totalAccounts: hierarchicalTrialBalance.companies.reduce(
        (sum, company) => sum + company.accountCounts.totalAccounts,
        0
      ),
      balancedCompanies: hierarchicalTrialBalance.companies.filter(
        (company) => company.balanceCheck.debitsEqualCredits
      ).length,
      dataQuality: {
        allConnected:
          hierarchicalTrialBalance.companies.length ===
          connectedXeroEntities.length,
        allBalanced: hierarchicalTrialBalance.companies.every(
          (company) => company.balanceCheck.debitsEqualCredits
        ),
        consolidatedBalanced:
          hierarchicalTrialBalance.consolidated.balanceCheck.debitsEqualCredits,
      },
    };

    console.log(
      `✅ Hierarchical consolidated trial balance completed for ${reportDate}:`,
      {
        companies: hierarchicalTrialBalance.companies.length,
        totalAccounts: hierarchicalTrialBalance.summary.totalAccounts,
        totalAssets: totals.totalAssets,
        consolidatedBalanced:
          hierarchicalTrialBalance.consolidated.balanceCheck.debitsEqualCredits,
      }
    );

    res.json(hierarchicalTrialBalance);
  } catch (error) {
    console.error(
      "❌ Error loading hierarchical consolidated trial balance:",
      error
    );
    res.status(500).json({
      error: "Failed to load hierarchical consolidated trial balance",
    });
  }
});

// DEBUG ENDPOINTS (Keep existing ones)
app.get("/api/debug/simple/:tenantId", async (req, res) => {
  try {
    const tokenData = await tokenStorage.getXeroToken(req.params.tenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    await xero.setTokenSet(tokenData);
    const response = await xero.accountingApi.getAccounts(req.params.tenantId);
    const allAccounts = response.body.accounts || [];
    const firstThree = allAccounts.slice(0, 3);

    res.json({
      message: "Raw Xero account data",
      totalAccounts: allAccounts.length,
      firstThreeAccounts: firstThree,
      firstAccountKeys: firstThree[0] ? Object.keys(firstThree[0]) : [],
    });
  } catch (error) {
    console.error("❌ Simple debug error:", error);
    res
      .status(500)
      .json({ error: "Simple debug failed", details: error.message });
  }
});

// TESTING - Balance Sheet endpoint (Keep for testing)
app.get("/api/trial-balance-fixed/:tenantId", async (req, res) => {
  try {
    const tokenData = await tokenStorage.getXeroToken(req.params.tenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    await xero.setTokenSet(tokenData);
    const response = await xero.accountingApi.getAccounts(req.params.tenantId);
    const allAccounts = response.body.accounts || [];

    const today = new Date().toISOString().split("T")[0];
    const balanceSheetResponse = await xero.accountingApi.getReportBalanceSheet(
      req.params.tenantId,
      today
    );

    console.error("✅ Got Balance Sheet report");

    res.json({
      message: "Testing Balance Sheet approach",
      totalAccounts: allAccounts.length,
      balanceSheetStructure:
        balanceSheetResponse.body.reports?.[0]?.rows?.slice(0, 10),
    });
  } catch (error) {
    console.error("❌ Error:", error);
    res
      .status(500)
      .json({ error: "Failed", details: error.message || "No message" });
  }
});

function toggleSectionFromElement(element) {
  const companyIndex = parseInt(element.getAttribute("data-company"));
  const sectionKey = element.getAttribute("data-section");
  toggleSection(companyIndex, sectionKey);
}

// Add these endpoints to your server.js after the existing endpoints

// ============================================================================
// ADDITIONAL FINANCIAL ANALYSIS ENDPOINTS
// ============================================================================

// Replace the existing profit-loss endpoint around line 1600
// Get Profit & Loss summary - UPDATED WITH CURRENT MONTH DEFAULT
app.get("/api/profit-loss/:tenantId", async (req, res) => {
  try {
    const tokenData = await tokenStorage.getXeroToken(req.params.tenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    await xero.setTokenSet(tokenData);

    // UPDATED DATE LOGIC - Default to current month
    const reportDate = req.query.date || new Date().toISOString().split("T")[0];
    const periodMonths = parseInt(req.query.periodMonths) || 1; // Default to 1 month instead of 12

    // Parse the report date properly
    const reportEndDate = new Date(reportDate);
    if (isNaN(reportEndDate.getTime())) {
      return res.status(400).json({ error: "Invalid report date provided" });
    }

    // Calculate from date based on period months
    let fromDate;
    if (periodMonths === 1) {
      // Current month - first day of current month
      fromDate = new Date(
        reportEndDate.getFullYear(),
        reportEndDate.getMonth(),
        1
      );
    } else {
      // Multi-month period - go back the specified number of months
      fromDate = new Date(reportEndDate);
      fromDate.setMonth(fromDate.getMonth() - (periodMonths - 1));
      fromDate.setDate(1);
    }

    const fromDateStr = fromDate.toISOString().split("T")[0];
    const actualReportDateStr = reportEndDate.toISOString().split("T")[0];

    console.log(
      `P&L Date Range: ${fromDateStr} to ${actualReportDateStr} (${periodMonths} month period)`
    );

    const response = await xero.accountingApi.getReportProfitAndLoss(
      req.params.tenantId,
      fromDateStr,
      actualReportDateStr // Use this instead of reportDate
    );

    const plRows = response.body.reports?.[0]?.rows || [];

    const plSummary = {
      totalRevenue: 0,
      totalExpenses: 0,
      netProfit: 0,
      revenueSections: {}, // New: group by section
      expenseSections: {}, // New: group by section
      revenueAccounts: [], // Keep for compatibility
      expenseAccounts: [], // Keep for compatibility
    };

    plRows.forEach((section) => {
      if (section.rowType === "Section" && section.rows && section.title) {
        const sectionTitle = section.title.toLowerCase();

        section.rows.forEach((row) => {
          if (row.rowType === "Row" && row.cells && row.cells.length >= 2) {
            const accountName = row.cells[0]?.value || "";
            const amount = parseFloat(row.cells[1]?.value || 0);

            if (accountName.toLowerCase().includes("total") || amount === 0) {
              return;
            }

            if (
              sectionTitle.includes("income") ||
              sectionTitle.includes("revenue") ||
              sectionTitle.includes("property maintenance") ||
              sectionTitle.includes("trading")
            ) {
              plSummary.revenueAccounts.push({
                name: accountName,
                amount: amount,
              });
              plSummary.totalRevenue += amount;
            } else if (
              sectionTitle.includes("expense") ||
              sectionTitle.includes("cost") ||
              sectionTitle.includes("administration") ||
              sectionTitle.includes("operating") ||
              sectionTitle.includes("salaries")
            ) {
              plSummary.expenseAccounts.push({
                name: accountName,
                amount: amount,
              });
              plSummary.totalExpenses += amount;
            }
          }
        });
      }
    });

    plSummary.netProfit = plSummary.totalRevenue - plSummary.totalExpenses;

    res.json({
      tenantId: req.params.tenantId,
      tenantName: tokenData.tenantName,
      period: {
        from: fromDateStr,
        to: actualReportDateStr, // Use the hardcoded August 31st date
        months: periodMonths,
        description:
          periodMonths === 1 ? "Current Month" : `${periodMonths} Month Period`,
      },
      summary: plSummary,
      generatedAt: new Date().toISOString(),
    });
  } catch (error) {
    console.error("Error getting P&L summary:", error);
    res.status(500).json({
      error: "Failed to get P&L summary",
      details: error.message,
    });
  }
});

// Also update the POST endpoint for P&L
app.post("/api/profit-loss-summary", async (req, res) => {
  try {
    const { organizationName, tenantId, date, periodMonths } = req.body;

    if (!organizationName && !tenantId) {
      return res
        .status(400)
        .json({ error: "Organization name or tenant ID required" });
    }

    // Find tenant ID if organization name provided
    let actualTenantId = tenantId;
    if (organizationName && !tenantId) {
      const connections = await tokenStorage.getAllXeroConnections();
      const connection = connections.find((c) =>
        c.tenantName.toLowerCase().includes(organizationName.toLowerCase())
      );
      if (connection) {
        actualTenantId = connection.tenantId;
      } else {
        return res.status(404).json({ error: "Organization not found" });
      }
    }

    // Build parameters with updated defaults
    const params = new URLSearchParams();
    if (date) params.append("date", date);
    // Default to 1 month instead of 12 for current month behavior
    params.append("periodMonths", (periodMonths || 1).toString());
    const queryString = params.toString()
      ? `?${params.toString()}`
      : "?periodMonths=1";

    const response = await fetch(
      `${req.protocol}://${req.get(
        "host"
      )}/api/profit-loss/${actualTenantId}${queryString}`
    );

    if (!response.ok) {
      throw new Error(`P&L request failed: ${response.status}`);
    }

    const result = await response.json();
    res.json(result);
  } catch (error) {
    console.error("❌ P&L summary API error:", error);
    res.status(500).json({ error: error.message });
  }
});

// Get aged receivables
app.get("/api/aged-receivables/:tenantId", async (req, res) => {
  try {
    const tokenData = await tokenStorage.getXeroToken(req.params.tenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    await xero.setTokenSet(tokenData);

    const reportDate = req.query.date || new Date().toISOString().split("T")[0];

    console.log(
      `Getting aged receivables for ${tokenData.tenantName} as at ${reportDate}`
    );

    const response = await xero.accountingApi.getReportAgedReceivablesByContact(
      req.params.tenantId,
      null, // contactId
      reportDate
    );

    const reportRows = response.body.reports?.[0]?.rows || [];

    const agedSummary = {
      totalOutstanding: 0,
      current: 0,
      days1to30: 0,
      days31to60: 0,
      days61to90: 0,
      over90days: 0,
      contactBreakdown: [],
    };

    reportRows.forEach((row) => {
      if (row.rowType === "Row" && row.cells && row.cells.length >= 6) {
        const contactName = row.cells[0]?.value || "";
        const total = parseFloat(row.cells[1]?.value || 0);
        const current = parseFloat(row.cells[2]?.value || 0);
        const days1to30 = parseFloat(row.cells[3]?.value || 0);
        const days31to60 = parseFloat(row.cells[4]?.value || 0);
        const days61to90 = parseFloat(row.cells[5]?.value || 0);
        const over90 = parseFloat(row.cells[6]?.value || 0);

        if (
          total > 0 &&
          contactName &&
          !contactName.toLowerCase().includes("total")
        ) {
          agedSummary.contactBreakdown.push({
            contactName,
            total,
            current,
            days1to30,
            days31to60,
            days61to90,
            over90days: over90,
            riskLevel:
              over90 > total * 0.3
                ? "HIGH"
                : days61to90 > total * 0.2
                ? "MEDIUM"
                : "LOW",
          });

          agedSummary.totalOutstanding += total;
          agedSummary.current += current;
          agedSummary.days1to30 += days1to30;
          agedSummary.days31to60 += days31to60;
          agedSummary.days61to90 += days61to90;
          agedSummary.over90days += over90;
        }
      }
    });

    // Sort by total outstanding (highest first)
    agedSummary.contactBreakdown.sort((a, b) => b.total - a.total);

    res.json({
      tenantId: req.params.tenantId,
      tenantName: tokenData.tenantName,
      reportDate,
      summary: agedSummary,
      riskAnalysis: {
        highRiskCustomers: agedSummary.contactBreakdown.filter(
          (c) => c.riskLevel === "HIGH"
        ).length,
        over90DaysPercentage: (
          (agedSummary.over90days / agedSummary.totalOutstanding) *
          100
        ).toFixed(1),
      },
      generatedAt: new Date().toISOString(),
    });
  } catch (error) {
    console.error("Error getting aged receivables:", error);
    res.status(500).json({
      error: "Failed to get aged receivables",
      details: error.message,
    });
  }
});

// Get expense analysis
app.get("/api/expense-analysis/:tenantId", async (req, res) => {
  try {
    const tokenData = await tokenStorage.getXeroToken(req.params.tenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    await xero.setTokenSet(tokenData);

    const reportDate = req.query.date || new Date().toISOString().split("T")[0];
    const periodMonths = parseInt(req.query.periodMonths) || 12;

    const fromDate = new Date(reportDate);
    fromDate.setMonth(fromDate.getMonth() - periodMonths);
    const fromDateStr = fromDate.toISOString().split("T")[0];

    console.log(`Getting expense analysis for ${tokenData.tenantName}`);
    const response = await xero.accountingApi.getReportProfitAndLoss(
      req.params.tenantId,
      fromDateStr,
      reportDate
    );

    console.log(
      "P&L API call successful:",
      response.body.reports?.[0] ? "YES" : "NO"
    );
    const plRows = response.body.reports?.[0]?.rows || [];

    const expenseAnalysis = {
      totalExpenses: 0,
      expenseCategories: [],
      topExpenses: [],
      monthlyAverage: 0,
    };

    plRows.forEach((section) => {
      if (section.rowType === "Section" && section.rows && section.title) {
        const sectionTitle = section.title.toLowerCase();

        if (sectionTitle.includes("expense") || sectionTitle.includes("cost")) {
          section.rows.forEach((row) => {
            if (row.rowType === "Row" && row.cells && row.cells.length >= 2) {
              const accountName = row.cells[0]?.value || "";
              const amount = parseFloat(row.cells[1]?.value || 0);

              if (!accountName.toLowerCase().includes("total") && amount > 0) {
                const expenseItem = {
                  accountName,
                  amount: amount,
                  monthlyAverage: Math.abs(amount) / periodMonths,
                  category: categorizeExpense(accountName),
                };

                expenseAnalysis.expenseCategories.push(expenseItem);
                expenseAnalysis.totalExpenses += Math.abs(amount);
              }
            }
          });
        }
      }
    });

    // Sort by amount (highest first)
    expenseAnalysis.expenseCategories.sort((a, b) => b.amount - a.amount);
    expenseAnalysis.topExpenses = expenseAnalysis.expenseCategories.slice(
      0,
      10
    );
    expenseAnalysis.monthlyAverage =
      expenseAnalysis.totalExpenses / periodMonths;

    // Group by category
    const categoryTotals = {};
    expenseAnalysis.expenseCategories.forEach((expense) => {
      if (!categoryTotals[expense.category]) {
        categoryTotals[expense.category] = 0;
      }
      categoryTotals[expense.category] += expense.amount;
    });

    expenseAnalysis.categoryBreakdown = Object.entries(categoryTotals)
      .map(([category, total]) => ({
        category,
        total,
        percentage: ((total / expenseAnalysis.totalExpenses) * 100).toFixed(1),
      }))
      .sort((a, b) => b.total - a.total);

    res.json({
      tenantId: req.params.tenantId,
      tenantName: tokenData.tenantName,
      period: {
        from: fromDateStr,
        to: reportDate,
        months: periodMonths,
      },
      analysis: expenseAnalysis,
      generatedAt: new Date().toISOString(),
    });
  } catch (error) {
    console.error("Error getting expense analysis:", error);
    res.status(500).json({
      error: "Failed to get expense analysis",
      details: error.message,
    });
  }
});

// Helper function to categorize expenses
function categorizeExpense(accountName) {
  const name = accountName.toLowerCase();

  if (
    name.includes("salary") ||
    name.includes("wage") ||
    name.includes("payroll")
  )
    return "Personnel";
  if (
    name.includes("rent") ||
    name.includes("lease") ||
    name.includes("utilities")
  )
    return "Occupancy";
  if (name.includes("marketing") || name.includes("advertising"))
    return "Marketing";
  if (name.includes("travel") || name.includes("transport")) return "Travel";
  if (name.includes("insurance")) return "Insurance";
  if (
    name.includes("legal") ||
    name.includes("professional") ||
    name.includes("consulting")
  )
    return "Professional Services";
  if (
    name.includes("equipment") ||
    name.includes("computer") ||
    name.includes("software")
  )
    return "Technology";
  if (name.includes("supplies") || name.includes("materials"))
    return "Supplies";
  if (name.includes("depreciation")) return "Depreciation";
  if (name.includes("interest") || name.includes("bank"))
    return "Finance Costs";

  return "Other";
}

// Get intercompany transactions
app.get("/api/intercompany/:tenantId", async (req, res) => {
  try {
    const tokenData = await tokenStorage.getXeroToken(req.params.tenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    await xero.setTokenSet(tokenData);

    const reportDate = req.query.date || new Date().toISOString().split("T")[0];

    console.log(
      `Getting intercompany transactions for ${tokenData.tenantName}`
    );

    // Get trial balance to find intercompany accounts
    const tbResponse = await xero.accountingApi.getReportBalanceSheet(
      req.params.tenantId,
      reportDate
    );

    const balanceSheetRows = tbResponse.body.reports?.[0]?.rows || [];

    const intercompanyAccounts = [];
    const racEntityNames = [
      "rirratjingu",
      "rac",
      "mining",
      "property",
      "enterprises",
      "invest",
      "ngarrkuwuy",
      "marrin",
      "yirrkala",
    ];

    balanceSheetRows.forEach((section) => {
      if (section.rowType === "Section" && section.rows) {
        section.rows.forEach((row) => {
          if (row.rowType === "Row" && row.cells && row.cells.length >= 2) {
            const accountName = row.cells[0]?.value || "";
            const balance = parseFloat(row.cells[1]?.value || 0);

            // Check if account name contains RAC entity names
            const isIntercompany = racEntityNames.some(
              (entity) =>
                accountName.toLowerCase().includes(entity) &&
                (accountName.toLowerCase().includes("loan") ||
                  accountName.toLowerCase().includes("due") ||
                  accountName.toLowerCase().includes("receivable") ||
                  accountName.toLowerCase().includes("payable"))
            );

            if (isIntercompany && Math.abs(balance) > 0) {
              intercompanyAccounts.push({
                accountName,
                balance,
                section: section.title,
                relatedEntity: racEntityNames.find((entity) =>
                  accountName.toLowerCase().includes(entity)
                ),
              });
            }
          }
        });
      }
    });

    const analysis = {
      totalIntercompanyAssets: intercompanyAccounts
        .filter(
          (acc) =>
            acc.balance > 0 && acc.section.toLowerCase().includes("asset")
        )
        .reduce((sum, acc) => sum + acc.balance, 0),
      totalIntercompanyLiabilities: intercompanyAccounts
        .filter(
          (acc) =>
            acc.balance > 0 && acc.section.toLowerCase().includes("liabilit")
        )
        .reduce((sum, acc) => sum + acc.balance, 0),
      accountCount: intercompanyAccounts.length,
      accounts: intercompanyAccounts.sort(
        (a, b) => Math.abs(b.balance) - Math.abs(a.balance)
      ),
    };

    res.json({
      tenantId: req.params.tenantId,
      tenantName: tokenData.tenantName,
      reportDate,
      analysis,
      generatedAt: new Date().toISOString(),
    });
  } catch (error) {
    console.error("Error getting intercompany transactions:", error);
    res.status(500).json({
      error: "Failed to get intercompany analysis",
      details: error.message,
    });
  }
});

// Get financial ratios
app.get("/api/financial-ratios/:tenantId", async (req, res) => {
  try {
    const tokenData = await tokenStorage.getXeroToken(req.params.tenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    const reportDate = req.query.date || new Date().toISOString().split("T")[0];

    // Get trial balance and P&L data
    const [tbResponse, plResponse] = await Promise.all([
      fetch(
        `${req.protocol}://${req.get("host")}/api/trial-balance/${
          req.params.tenantId
        }?date=${reportDate}`
      ),
      fetch(
        `${req.protocol}://${req.get("host")}/api/profit-loss/${
          req.params.tenantId
        }?date=${reportDate}`
      ),
    ]);

    if (!tbResponse.ok || !plResponse.ok) {
      throw new Error(
        "Failed to retrieve financial data for ratio calculation"
      );
    }

    const [tbData, plData] = await Promise.all([
      tbResponse.json(),
      plResponse.json(),
    ]);

    const totals = tbData.trialBalance.totals;
    const plSummary = plData.summary;

    // Calculate key financial ratios
    const ratios = {
      liquidity: {
        currentRatio: totals.totalAssets / Math.max(totals.totalLiabilities, 1),
        workingCapital: totals.totalAssets - totals.totalLiabilities,
      },
      leverage: {
        debtToEquity:
          Math.abs(totals.totalLiabilities) / Math.max(totals.totalEquity, 1),
        equityRatio: totals.totalEquity / Math.max(totals.totalAssets, 1),
      },
      profitability: {
        netProfitMargin:
          (plSummary.netProfit / Math.max(plSummary.totalRevenue, 1)) * 100,
        returnOnAssets:
          (plSummary.netProfit / Math.max(totals.totalAssets, 1)) * 100,
        returnOnEquity:
          (plSummary.netProfit / Math.max(totals.totalEquity, 1)) * 100,
      },
      efficiency: {
        assetTurnover: plSummary.totalRevenue / Math.max(totals.totalAssets, 1),
        expenseRatio:
          (plSummary.totalExpenses / Math.max(plSummary.totalRevenue, 1)) * 100,
      },
    };

    // Add ratio interpretations
    const interpretations = {
      currentRatio:
        ratios.liquidity.currentRatio > 2
          ? "Strong"
          : ratios.liquidity.currentRatio > 1
          ? "Adequate"
          : "Concerning",
      debtToEquity:
        ratios.leverage.debtToEquity < 0.3
          ? "Conservative"
          : ratios.leverage.debtToEquity < 1
          ? "Moderate"
          : "High",
      profitability:
        ratios.profitability.netProfitMargin > 10
          ? "Excellent"
          : ratios.profitability.netProfitMargin > 5
          ? "Good"
          : ratios.profitability.netProfitMargin > 0
          ? "Break-even"
          : "Loss",
    };

    res.json({
      tenantId: req.params.tenantId,
      tenantName: tokenData.tenantName,
      reportDate,
      ratios,
      interpretations,
      dataSource: {
        totalAssets: totals.totalAssets,
        totalLiabilities: totals.totalLiabilities,
        totalEquity: totals.totalEquity,
        totalRevenue: plSummary.totalRevenue,
        totalExpenses: plSummary.totalExpenses,
        netProfit: plSummary.netProfit,
      },
      generatedAt: new Date().toISOString(),
    });
  } catch (error) {
    console.error("Error calculating financial ratios:", error);
    res.status(500).json({
      error: "Failed to calculate financial ratios",
      details: error.message,
    });
  }
});

// ============================================================================
// MONTHLY BREAKDOWN ENDPOINT (for Monthly Card reconciliation)
// ============================================================================

app.get("/api/monthly-breakdown/:tenantId", async (req, res) => {
  try {
    const tokenData = await tokenStorage.getXeroToken(req.params.tenantId);
    if (!tokenData) {
      return res
        .status(404)
        .json({ error: "Tenant not found or token expired" });
    }

    const reportDate = req.query.date || new Date().toISOString().split("T")[0];

    console.log(
      `📅 DEBUG: Starting monthly breakdown for ${tokenData.tenantName}`
    );
    console.log(`📅 DEBUG: Report date: ${reportDate}`);

    // Use IDENTICAL date logic to YoY analysis
    const currentPeriodEnd = new Date(reportDate);
    const currentPeriodStart = new Date(currentPeriodEnd);
    currentPeriodStart.setFullYear(currentPeriodEnd.getFullYear() - 1);
    currentPeriodStart.setDate(currentPeriodStart.getDate() + 1);

    console.log(
      `📅 DEBUG: YoY period: ${
        currentPeriodStart.toISOString().split("T")[0]
      } to ${currentPeriodEnd.toISOString().split("T")[0]}`
    );

    // Calculate 12 complete calendar months ending with report month
    const monthlyPeriods = [];
    const reportMonth = new Date(reportDate);

    for (let i = 11; i >= 0; i--) {
      const monthDate = new Date(reportMonth);
      monthDate.setMonth(reportMonth.getMonth() - i);

      // Always use complete calendar month (1st to last day)
      const monthStart = new Date(
        monthDate.getFullYear(),
        monthDate.getMonth(),
        1
      );
      const monthEnd = new Date(
        monthDate.getFullYear(),
        monthDate.getMonth() + 1,
        0
      );

      const monthStartStr = monthStart.toISOString().split("T")[0];
      const monthEndStr = monthEnd.toISOString().split("T")[0];

      monthlyPeriods.push({
        monthIndex: i + 1,
        label: monthStart.toLocaleDateString("en-US", {
          month: "short",
          year: "numeric",
        }),
        startDate: monthStartStr,
        endDate: monthEndStr,
        dayCount:
          Math.ceil((monthEnd - monthStart) / (1000 * 60 * 60 * 24)) + 1,
      });
    }

    console.log(
      `DEBUG: Created ${monthlyPeriods.length} calendar month periods:`
    );
    monthlyPeriods.forEach((period, idx) => {
      console.log(
        `  Month ${idx + 1}: ${period.startDate} to ${period.endDate} (${
          period.dayCount
        } days) - ${period.label}`
      );
    });

    await xero.setTokenSet(tokenData);

    // Get monthly P&L data for each period
    const monthlyData = [];
    let totalRevenue = 0;
    let totalExpenses = 0;

    for (const period of monthlyPeriods) {
      try {
        console.log(
          `📊 DEBUG: Fetching P&L for ${period.label} (${period.startDate} to ${period.endDate})`
        );

        const response = await xero.accountingApi.getReportProfitAndLoss(
          req.params.tenantId,
          period.startDate,
          period.endDate
        );

        const plRows = response.body.reports?.[0]?.rows || [];
        const monthlyPL = parsePLData(plRows);

        monthlyData.push({
          ...period,
          revenue: monthlyPL.totalRevenue,
          expenses: monthlyPL.totalExpenses,
          netProfit: monthlyPL.totalRevenue - monthlyPL.totalExpenses,
          revenueAccounts: monthlyPL.revenueAccounts.length,
          expenseAccounts: monthlyPL.expenseAccounts.length,
        });

        totalRevenue += monthlyPL.totalRevenue;
        totalExpenses += monthlyPL.totalExpenses;

        console.log(
          `  ✅ ${
            period.label
          }: Rev $${monthlyPL.totalRevenue.toLocaleString()}, Exp $${monthlyPL.totalExpenses.toLocaleString()}, Profit $${(
            monthlyPL.totalRevenue - monthlyPL.totalExpenses
          ).toLocaleString()}`
        );
      } catch (monthError) {
        console.error(
          `❌ ERROR loading month ${period.label}:`,
          monthError.message
        );

        monthlyData.push({
          ...period,
          revenue: 0,
          expenses: 0,
          netProfit: 0,
          revenueAccounts: 0,
          expenseAccounts: 0,
          error: monthError.message,
        });
      }
    }

    // Calculate totals for reconciliation
    const monthlyTotals = {
      totalRevenue,
      totalExpenses,
      totalNetProfit: totalRevenue - totalExpenses,
    };

    console.log(`📊 DEBUG: Monthly breakdown totals:`);
    console.log(
      `  Total Revenue: $${monthlyTotals.totalRevenue.toLocaleString()}`
    );
    console.log(
      `  Total Expenses: $${monthlyTotals.totalExpenses.toLocaleString()}`
    );
    console.log(
      `  Total Net Profit: $${monthlyTotals.totalNetProfit.toLocaleString()}`
    );

    // Get YoY data for reconciliation check
    let yoyComparison = null;
    try {
      const yoyParams = req.query.date ? `?date=${req.query.date}` : "";
      const yoyResponse = await fetch(
        `${req.protocol}://${req.get("host")}/api/yoy-analysis/${
          req.params.tenantId
        }${yoyParams}`
      );

      if (yoyResponse.ok) {
        const yoyData = await yoyResponse.json();
        const currentPeriod = yoyData.analysis.periods.current;

        yoyComparison = {
          yoyRevenue: currentPeriod.revenue,
          yoyExpenses: currentPeriod.revenue - currentPeriod.profit, // Calculate from revenue and profit
          yoyNetProfit: currentPeriod.profit,

          // Reconciliation checks
          revenueVariance: monthlyTotals.totalRevenue - currentPeriod.revenue,
          expenseVariance:
            monthlyTotals.totalExpenses -
            (currentPeriod.revenue - currentPeriod.profit),
          profitVariance: monthlyTotals.totalNetProfit - currentPeriod.profit,

          // Status flags
          revenueReconciled:
            Math.abs(monthlyTotals.totalRevenue - currentPeriod.revenue) < 100,
          expenseReconciled:
            Math.abs(
              monthlyTotals.totalExpenses -
                (currentPeriod.revenue - currentPeriod.profit)
            ) < 100,
          profitReconciled:
            Math.abs(monthlyTotals.totalNetProfit - currentPeriod.profit) < 100,
        };

        console.log(`🔍 DEBUG: YoY Reconciliation Check:`);
        console.log(
          `  YoY Revenue: $${yoyComparison.yoyRevenue.toLocaleString()}, Monthly Sum: $${monthlyTotals.totalRevenue.toLocaleString()}, Variance: $${yoyComparison.revenueVariance.toLocaleString()}`
        );
        console.log(
          `  YoY Profit: $${yoyComparison.yoyNetProfit.toLocaleString()}, Monthly Sum: $${monthlyTotals.totalNetProfit.toLocaleString()}, Variance: $${yoyComparison.profitVariance.toLocaleString()}`
        );
        console.log(
          `  Revenue Reconciled: ${
            yoyComparison.revenueReconciled ? "✅" : "❌"
          }`
        );
        console.log(
          `  Profit Reconciled: ${yoyComparison.profitReconciled ? "✅" : "❌"}`
        );
      }
    } catch (yoyError) {
      console.warn(
        `⚠️ Could not fetch YoY data for reconciliation:`,
        yoyError.message
      );
    }

    res.json({
      tenantId: req.params.tenantId,
      tenantName: tokenData.tenantName,
      reportDate,
      period: {
        start: currentPeriodStart.toISOString().split("T")[0],
        end: currentPeriodEnd.toISOString().split("T")[0],
        totalDays: Math.ceil(
          (currentPeriodEnd - currentPeriodStart) / (1000 * 60 * 60 * 24)
        ),
      },
      monthlyBreakdown: monthlyData,
      totals: monthlyTotals,
      yoyReconciliation: yoyComparison,
      generatedAt: new Date().toISOString(),
      debugInfo: {
        monthlyPeriodsCount: monthlyPeriods.length,
        errorMonths: monthlyData.filter((m) => m.error).length,
        reconciliationStatus: yoyComparison
          ? {
              overall:
                yoyComparison.revenueReconciled &&
                yoyComparison.profitReconciled
                  ? "RECONCILED"
                  : "VARIANCE_DETECTED",
              largestVariance: Math.max(
                Math.abs(yoyComparison.revenueVariance || 0),
                Math.abs(yoyComparison.profitVariance || 0)
              ),
            }
          : "YOY_DATA_UNAVAILABLE",
      },
    });
  } catch (error) {
    console.error("❌ ERROR in monthly breakdown:", error);
    res.status(500).json({
      error: "Failed to get monthly breakdown",
      details: error.message,
      tenantId: req.params.tenantId,
    });
  }
});

// POST wrapper for Monthly Breakdown
app.post("/api/monthly-breakdown", async (req, res) => {
  try {
    const { organizationName, tenantId, date } = req.body;

    if (!organizationName && !tenantId) {
      return res
        .status(400)
        .json({ error: "Organization name or tenant ID required" });
    }

    let actualTenantId = tenantId;
    if (organizationName && !tenantId) {
      const connections = await tokenStorage.getAllXeroConnections();
      const connection = connections.find((c) =>
        c.tenantName.toLowerCase().includes(organizationName.toLowerCase())
      );
      if (connection) {
        actualTenantId = connection.tenantId;
      } else {
        return res.status(404).json({ error: "Organization not found" });
      }
    }

    const dateParam = date ? `?date=${date}` : "";
    const response = await fetch(
      `${req.protocol}://${req.get(
        "host"
      )}/api/monthly-breakdown/${actualTenantId}${dateParam}`
    );

    if (!response.ok) {
      throw new Error(`Monthly breakdown request failed: ${response.status}`);
    }

    const result = await response.json();
    res.json(result);
  } catch (error) {
    console.error("❌ Monthly breakdown API error:", error);
    res.status(500).json({ error: error.message });
  }
});

// Initialize database and start server
async function startServer() {
  try {
    await initializeAutoRefresh();

    app.listen(port, () => {
      console.log(`🚀 RAC Financial Dashboard running on port ${port}`);
      console.log(
        `📊 Dashboard: ${
          process.env.NODE_ENV === "production"
            ? "https://your-app.up.railway.app"
            : `http://localhost:${port}`
        }`
      );
      console.log(`💾 Database: Connected to PostgreSQL`);
      console.log(`🔗 Xero OAuth: /auth`);
      console.log(`🔗 ApprovalMax OAuth: /auth?provider=approvalmax`);
      console.log(
        `🎯 Ready for RAC financial integration with date-flexible trial balance!`
      );
    });
  } catch (error) {
    console.error("❌ Failed to start server:", error);
    process.exit(1);
  }
}

startServer();
