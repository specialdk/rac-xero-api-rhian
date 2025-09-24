// session-data-manager.js
// Step 2: Session Data Manager for RAC Financial Dashboard
// Handles 30-minute session data with instant consolidation

class SessionDataManager {
  constructor(pool, xero, tokenStorage) {
    this.pool = pool;
    this.xero = xero;
    this.tokenStorage = tokenStorage;
    this.sessionTTL = 30 * 60 * 1000; // 30 minutes to match Xero connection expiry
  }

  // Initialize - call this once when server starts
  async initialize() {
    try {
      console.log("✅ Session data manager initialized");
      return true;
    } catch (error) {
      console.error("❌ Session manager initialization failed:", error);
      return false;
    }
  }

  // MAIN METHOD: Load all company data in parallel (expensive operation)
  async loadSessionData(sessionId, connectedTenantIds) {
    console.log(
      `🚀 Loading session data for ${connectedTenantIds.length} companies`
    );
    const expiresAt = new Date(Date.now() + this.sessionTTL);

    // Clear any existing session data first
    await this.pool.query(
      "DELETE FROM session_company_data WHERE session_id = $1",
      [sessionId]
    );

    // Load all companies in parallel - this is where the speed improvement happens
    const loadPromises = connectedTenantIds.map((tenantId) =>
      this.loadSingleCompanyData(sessionId, tenantId, expiresAt)
    );

    const results = await Promise.all(loadPromises);

    // Set initial display selection (all connected companies)
    await this.setDisplaySelection(sessionId, connectedTenantIds, "overview");

    const successCount = results.filter((r) => r.success).length;
    console.log(
      `✅ Session loaded: ${successCount}/${connectedTenantIds.length} companies successful`
    );

    return {
      sessionId,
      totalCompanies: connectedTenantIds.length,
      successfulCompanies: successCount,
      expiresAt: expiresAt.toISOString(),
    };
  }

  // Load single company data (called in parallel)
  async loadSingleCompanyData(sessionId, tenantId, expiresAt) {
    try {
      const tokenData = await this.tokenStorage.getXeroToken(tenantId);
      if (!tokenData) {
        throw new Error(`No token found for tenant ${tenantId}`);
      }

      await this.xero.setTokenSet(tokenData);

      // Load key financial data in parallel
      const [trialBalanceData, cashData] = await Promise.all([
        this.loadTrialBalanceTotals(tenantId).catch(() => null),
        this.loadCashPosition(tenantId).catch(() => null),
      ]);

      // Extract and store key metrics
      const companyData = {
        session_id: sessionId,
        tenant_id: tenantId,
        tenant_name: tokenData.tenantName,
        total_assets: trialBalanceData?.totalAssets || 0,
        total_liabilities: Math.abs(trialBalanceData?.totalLiabilities || 0),
        total_equity: trialBalanceData?.totalEquity || 0,
        total_cash: cashData?.totalCash || 0,
        total_revenue: 0, // We'll add P&L in later steps
        total_expenses: 0,
        net_profit: 0,
        is_balanced: trialBalanceData?.isBalanced || false,
        has_data: !!(trialBalanceData || cashData),
        load_error: null,
        expires_at: expiresAt,
      };

      // Store in database
      await this.pool.query(
        `
        INSERT INTO session_company_data (
          session_id, tenant_id, tenant_name, total_assets, total_liabilities, 
          total_equity, total_cash, total_revenue, total_expenses, net_profit,
          is_balanced, has_data, load_error, expires_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
      `,
        [
          companyData.session_id,
          companyData.tenant_id,
          companyData.tenant_name,
          companyData.total_assets,
          companyData.total_liabilities,
          companyData.total_equity,
          companyData.total_cash,
          companyData.total_revenue,
          companyData.total_expenses,
          companyData.net_profit,
          companyData.is_balanced,
          companyData.has_data,
          companyData.load_error,
          companyData.expires_at,
        ]
      );

      console.log(`✅ Loaded data for ${tokenData.tenantName}`);
      return { success: true, tenantId, data: companyData };
    } catch (error) {
      console.error(`❌ Failed to load data for ${tenantId}:`, error);

      // Store error record so we know what failed
      await this.pool.query(
        `
        INSERT INTO session_company_data (
          session_id, tenant_id, tenant_name, has_data, load_error, expires_at
        ) VALUES ($1, $2, $3, $4, $5, $6)
      `,
        [
          sessionId,
          tenantId,
          "Unknown Company",
          false,
          error.message,
          expiresAt,
        ]
      );

      return { success: false, tenantId, error: error.message };
    }
  }

  // INSTANT consolidation from cached session data (fast operation)
  async getConsolidatedData(sessionId, selectedTenantIds = null) {
    // Get current selection if none provided
    if (!selectedTenantIds) {
      const selection = await this.getDisplaySelection(sessionId);
      selectedTenantIds = selection.selected_tenant_ids;
    }

    if (!selectedTenantIds || selectedTenantIds.length === 0) {
      return { error: "No companies selected" };
    }

    // Fast database query - no API calls, just math
    const result = await this.pool.query(
      `
      SELECT 
        tenant_id,
        tenant_name,
        total_assets,
        total_liabilities, 
        total_equity,
        total_cash,
        total_revenue,
        total_expenses,
        net_profit,
        is_balanced,
        has_data,
        load_error
      FROM session_company_data 
      WHERE session_id = $1 AND tenant_id = ANY($2)
      AND expires_at > NOW()
    `,
      [sessionId, selectedTenantIds]
    );

    const companies = result.rows;

    if (companies.length === 0) {
      return { error: "No session data found or session expired" };
    }

    // Fast consolidation math - pure JavaScript, no external calls
    const consolidated = {
      totals: {
        totalAssets: companies.reduce(
          (sum, c) => sum + parseFloat(c.total_assets || 0),
          0
        ),
        totalLiabilities: companies.reduce(
          (sum, c) => sum + parseFloat(c.total_liabilities || 0),
          0
        ),
        totalEquity: companies.reduce(
          (sum, c) => sum + parseFloat(c.total_equity || 0),
          0
        ),
        totalCash: companies.reduce(
          (sum, c) => sum + parseFloat(c.total_cash || 0),
          0
        ),
        totalRevenue: companies.reduce(
          (sum, c) => sum + parseFloat(c.total_revenue || 0),
          0
        ),
        totalExpenses: companies.reduce(
          (sum, c) => sum + parseFloat(c.total_expenses || 0),
          0
        ),
        totalNetProfit: companies.reduce(
          (sum, c) => sum + parseFloat(c.net_profit || 0),
          0
        ),
      },
      companies: companies.map((c) => ({
        tenantId: c.tenant_id,
        tenantName: c.tenant_name,
        totalAssets: parseFloat(c.total_assets || 0),
        totalLiabilities: parseFloat(c.total_liabilities || 0),
        totalEquity: parseFloat(c.total_equity || 0),
        totalCash: parseFloat(c.total_cash || 0),
        isBalanced: c.is_balanced,
        hasData: c.has_data,
        error: c.load_error,
      })),
      summary: {
        totalCompanies: companies.length,
        balancedCompanies: companies.filter((c) => c.is_balanced).length,
        companiesWithData: companies.filter((c) => c.has_data).length,
        companiesWithErrors: companies.filter((c) => c.load_error).length,
      },
    };

    return consolidated;
  }

  // Update display selection (instant filtering)
  async setDisplaySelection(
    sessionId,
    selectedTenantIds,
    currentView = "overview"
  ) {
    await this.pool.query(
      `
      INSERT INTO user_display_selection (session_id, selected_tenant_ids, current_view, last_updated)
      VALUES ($1, $2, $3, NOW())
      ON CONFLICT (session_id)
      DO UPDATE SET 
        selected_tenant_ids = $2,
        current_view = $3,
        last_updated = NOW()
    `,
      [sessionId, selectedTenantIds, currentView]
    );

    console.log(
      `✅ Display selection updated: ${selectedTenantIds.length} companies for ${currentView}`
    );
  }

  // Get current display selection
  async getDisplaySelection(sessionId) {
    const result = await this.pool.query(
      "SELECT selected_tenant_ids, current_view FROM user_display_selection WHERE session_id = $1",
      [sessionId]
    );

    return (
      result.rows[0] || { selected_tenant_ids: [], current_view: "overview" }
    );
  }

  // Helper methods for loading Xero data
  async loadTrialBalanceTotals(tenantId, reportDate = null) {
    const date = reportDate || new Date().toISOString().split("T")[0];

    const response = await this.xero.accountingApi.getReportBalanceSheet(
      tenantId,
      date
    );
    const balanceSheetRows = response.body.reports?.[0]?.rows || [];

    let totalAssets = 0;
    let totalLiabilities = 0;
    let totalEquity = 0;

    // Extract totals from balance sheet
    balanceSheetRows.forEach((section) => {
      if (section.rowType === "Section" && section.rows && section.title) {
        const sectionTitle = section.title.toLowerCase();

        section.rows.forEach((row) => {
          if (row.rowType === "Row" && row.cells && row.cells.length >= 2) {
            const amount = parseFloat(row.cells[1]?.value || 0);
            if (amount === 0) return;

            if (sectionTitle.includes("asset")) {
              totalAssets += amount;
            } else if (sectionTitle.includes("liabilit")) {
              totalLiabilities += Math.abs(amount);
            } else if (sectionTitle.includes("equity")) {
              totalEquity += amount;
            }
          }
        });
      }
    });

    // Simple balance check
    const isBalanced =
      Math.abs(totalAssets - (totalLiabilities + totalEquity)) < 1.0;

    return { totalAssets, totalLiabilities, totalEquity, isBalanced };
  }

  async loadCashPosition(tenantId) {
    const response = await this.xero.accountingApi.getReportBankSummary(
      tenantId
    );
    const bankSummaryRows = response.body.reports?.[0]?.rows || [];

    let totalCash = 0;

    bankSummaryRows.forEach((row) => {
      if (row.rowType === "Section" && row.rows) {
        row.rows.forEach((bankRow) => {
          if (
            bankRow.rowType === "Row" &&
            bankRow.cells &&
            bankRow.cells.length >= 5
          ) {
            const accountName = bankRow.cells[0]?.value || "";
            if (accountName && !accountName.toLowerCase().includes("total")) {
              totalCash += parseFloat(bankRow.cells[4]?.value || 0);
            }
          }
        });
      }
    });

    return { totalCash };
  }

  // Session expiry check
  async hasValidSessionData(sessionId) {
    const result = await this.pool.query(
      `
      SELECT COUNT(*) as count, MIN(expires_at) as earliest_expiry
      FROM session_company_data 
      WHERE session_id = $1 AND expires_at > NOW()
    `,
      [sessionId]
    );

    const row = result.rows[0];
    return {
      hasData: parseInt(row.count) > 0,
      companiesCount: parseInt(row.count),
      expiresAt: row.earliest_expiry,
    };
  }
}

export default SessionDataManager;
