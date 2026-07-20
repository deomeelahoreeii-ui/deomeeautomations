import { ActionError, defineAction } from "astro:actions";
import { z } from "astro/zod";

const API_BASE = import.meta.env.PUBLIC_API_BASE_URL || "http://127.0.0.1:8020";

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...init?.headers },
    ...init,
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    const detail = Array.isArray(body.detail)
      ? body.detail.map((item: any) => item.msg || item.message || JSON.stringify(item)).join("; ")
      : typeof body.detail === "string"
        ? body.detail
        : body.detail?.message || response.statusText;
    throw new ActionError({
      code: response.status === 404 ? "NOT_FOUND" : "BAD_REQUEST",
      message: detail,
    });
  }
  return response.json() as Promise<T>;
}

const schoolInput = z.object({
  id: z.string().optional(),
  emis: z.string().min(1),
  name: z.string().min(1),
  district_ref: z.string().min(1),
  department_ref: z.string().min(1),
  wing_ref: z.string().min(1),
  tehsil_ref: z.string().min(1),
  markaz_ref: z.string().default(""),
  head_name: z.string().default(""),
  head_contact: z.string().default(""),
  shift: z.string().default(""),
  school_type: z.string().default(""),
  school_level: z.string().default(""),
  deos_wise: z.string().default(""),
  notes: z.string().default(""),
  active: z.boolean().default(true),
});

const officerInput = z.object({
  id: z.string().optional(),
  role: z.enum(["aeo", "ddeo"]),
  name: z.string().min(1),
  mobile: z.string().min(1),
  district_ref: z.string().min(1),
  department_ref: z.string().min(1),
  wing_ref: z.string().min(1),
  tehsil_ref: z.string().min(1),
  markaz_ref: z.string().default(""),
  jurisdiction_refs: z.array(z.string().uuid()).optional(),
  replace_conflicts: z.boolean().default(false),
  helpdesk_user_email: z.string().email().or(z.literal("")).default(""),
  helpdesk_enabled: z.boolean().default(false),
  active: z.boolean().default(true),
});

const activityRuleInput = z.object({
  name: z.string().min(1).max(180),
  classification: z.literal("review_required").default("review_required"),
  match_mode: z.enum(["all", "any"]).default("all"),
  distance_enabled: z.boolean().default(true),
  distance_operator: z.enum(["gt", "gte"]).default("gte"),
  distance_threshold_meters: z.number().min(0).max(100000).default(50),
  time_enabled: z.boolean().default(false),
  time_operator: z.enum(["between", "outside"]).default("between"),
  time_start: z.string().regex(/^([01]\d|2[0-3]):[0-5]\d$/).default("00:00"),
  time_end: z.string().regex(/^([01]\d|2[0-3]):[0-5]\d$/).default("07:00"),
  timezone: z.literal("Asia/Karachi").default("Asia/Karachi"),
  created_by: z.string().max(120).default("web-operator"),
});

const simpleActivityRuleInput = z.object({
  name: z.string().min(1).max(180),
  operator: z.enum(["lt", "lte"]).default("lt"),
  minimum_seconds: z.number().int().min(1).max(86400).default(300),
  timezone: z.literal("Asia/Karachi").default("Asia/Karachi"),
  created_by: z.string().max(120).default("web-operator"),
});

export const server = {
  notifications: {
    overview: defineAction({ handler: () => api("/api/v1/notifications/overview") }),
    deliveries: defineAction({
      input: z.object({
        status: z.string().default(""),
        eventType: z.string().default(""),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(25),
      }),
      handler: (input) => {
        const params = new URLSearchParams({
          status: input.status,
          event_type: input.eventType,
          page: String(input.page),
          page_size: String(input.pageSize),
        });
        return api(`/api/v1/notifications/deliveries?${params}`);
      },
    }),
    sendTest: defineAction({
      input: z.object({
        channel: z.literal("antidengue").default("antidengue"),
        message: z.string().min(1).max(500).default("Automation Platform notification test"),
      }),
      handler: (input) => api("/api/v1/notifications/test", {
        method: "POST",
        body: JSON.stringify(input),
      }),
    }),
  },
  storage: {
    overview: defineAction({ handler: () => api("/api/v1/storage/overview") }),
    artifacts: defineAction({
      input: z.object({
        moduleKey: z.string().default(""),
        status: z.string().default(""),
        search: z.string().default(""),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(25),
      }),
      handler: (input) => {
        const params = new URLSearchParams({
          module_key: input.moduleKey,
          storage_status: input.status,
          search: input.search,
          page: String(input.page),
          page_size: String(input.pageSize),
        });
        return api(`/api/v1/storage/artifacts?${params}`);
      },
    }),
    retryArtifact: defineAction({
      input: z.object({ id: z.number().int().positive() }),
      handler: ({ id }) => api(`/api/v1/storage/artifacts/${id}/retry`, { method: "POST" }),
    }),
    backfill: defineAction({
      input: z.object({ apply: z.boolean().default(false), moduleKey: z.string().optional(), limit: z.number().int().min(1).max(2000).default(200) }),
      handler: ({ apply, moduleKey, limit }) => api("/api/v1/storage/backfill", {
        method: "POST",
        body: JSON.stringify({ apply, module_key: moduleKey || null, limit }),
      }),
    }),
  },
  antidengue: {
    simpleActivityRules: defineAction({ handler: () => api("/api/v1/antidengue/simple-activity-rules") }),
    createSimpleActivityRule: defineAction({ input: simpleActivityRuleInput, handler: (input) => api("/api/v1/antidengue/simple-activity-rules", { method: "POST", body: JSON.stringify(input) }) }),
    updateSimpleActivityRule: defineAction({ input: simpleActivityRuleInput.extend({ id: z.string().uuid() }), handler: ({ id, ...input }) => api(`/api/v1/antidengue/simple-activity-rules/${id}`, { method: "PATCH", body: JSON.stringify(input) }) }),
    createSimpleActivityRuleVersion: defineAction({ input: z.object({ id: z.string().uuid() }), handler: ({ id }) => api(`/api/v1/antidengue/simple-activity-rules/${id}/versions`, { method: "POST" }) }),
    publishSimpleActivityRule: defineAction({ input: z.object({ id: z.string().uuid() }), handler: ({ id }) => api(`/api/v1/antidengue/simple-activity-rules/${id}/publish`, { method: "POST" }) }),
    setSimpleActivityRuleEnabled: defineAction({ input: z.object({ id: z.string().uuid(), enabled: z.boolean() }), handler: ({ id, enabled }) => api(`/api/v1/antidengue/simple-activity-rules/${id}/enabled?enabled=${enabled}`, { method: "POST" }) }),
    archiveSimpleActivityRule: defineAction({ input: z.object({ id: z.string().uuid() }), handler: ({ id }) => api(`/api/v1/antidengue/simple-activity-rules/${id}`, { method: "DELETE" }) }),
    previewSimpleActivityRule: defineAction({ input: simpleActivityRuleInput.extend({ sample_size: z.number().int().min(1).max(100).default(20) }), handler: (input) => api("/api/v1/antidengue/simple-activity-rules/previews/evaluate", { method: "POST", body: JSON.stringify(input) }) }),
    activityRules: defineAction({ handler: () => api("/api/v1/antidengue/activity-rules") }),
    createActivityRule: defineAction({
      input: activityRuleInput,
      handler: (input) => api("/api/v1/antidengue/activity-rules", { method: "POST", body: JSON.stringify(input) }),
    }),
    updateActivityRule: defineAction({
      input: activityRuleInput.extend({ id: z.string().uuid() }),
      handler: ({ id, ...input }) => api(`/api/v1/antidengue/activity-rules/${id}`, { method: "PATCH", body: JSON.stringify(input) }),
    }),
    createActivityRuleVersion: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/antidengue/activity-rules/${id}/versions`, { method: "POST" }),
    }),
    publishActivityRule: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/antidengue/activity-rules/${id}/publish`, { method: "POST" }),
    }),
    setActivityRuleEnabled: defineAction({
      input: z.object({ id: z.string().uuid(), enabled: z.boolean() }),
      handler: ({ id, enabled }) => api(`/api/v1/antidengue/activity-rules/${id}/enabled?enabled=${enabled}`, { method: "POST" }),
    }),
    archiveActivityRule: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/antidengue/activity-rules/${id}`, { method: "DELETE" }),
    }),
    previewActivityRule: defineAction({
      input: activityRuleInput.extend({ sample_size: z.number().int().min(1).max(100).default(20) }),
      handler: (input) => api("/api/v1/antidengue/activity-rules/previews/evaluate", { method: "POST", body: JSON.stringify(input) }),
    }),
    manualReports: defineAction({
      input: z.object({
        search: z.string().default(""),
        status: z.string().default(""),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(10),
      }),
      handler: (input) => {
        const params = new URLSearchParams({
          search: input.search,
          validation_status: input.status,
          page: String(input.page),
          page_size: String(input.pageSize),
        });
        return api(`/api/v1/antidengue/manual-reports?${params}`);
      },
    }),
    processManualReport: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/antidengue/manual-reports/${id}/process`, { method: "POST" }),
    }),
    hardDeleteManualReport: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/antidengue/manual-reports/${id}/hard`, { method: "DELETE" }),
    }),
  },
  crm: {
    caseStatistics: defineAction({
      input: z.object({}),
      handler: () => api("/api/v1/crm/cases/statistics"),
    }),
    cases: defineAction({
      input: z.object({ search: z.string().default(""), state: z.string().default(""), limit: z.number().int().min(1).max(200).default(100) }),
      handler: (input) => {
        const params = new URLSearchParams({ search: input.search, state: input.state, limit: String(input.limit) });
        return api(`/api/v1/crm/cases?${params}`);
      },
    }),
    complaintCase: defineAction({ input: z.object({ id: z.string().uuid() }), handler: ({id}) => api(`/api/v1/crm/cases/${id}`) }),
    reviewComplaintCase: defineAction({
      input: z.object({ id: z.string().uuid(), fields: z.record(z.string(), z.string().nullable()), accepted_document_ids: z.array(z.string().uuid()), rejected_document_ids: z.array(z.string().uuid()) }),
      handler: ({id, ...body}) => api(`/api/v1/crm/cases/${id}/review`, { method: "PATCH", body: JSON.stringify(body) }),
    }),
    reviewComplaintDocument: defineAction({
      input: z.object({ caseId: z.string().uuid(), documentId: z.string().uuid(), role: z.enum(["main_complaint", "complaint_details", "attachment", "reply", "report"]), accepted: z.boolean() }),
      handler: ({caseId, documentId, ...body}) => api(`/api/v1/crm/cases/${caseId}/documents/${documentId}`, { method: "PATCH", body: JSON.stringify(body) }),
    }),
    approveFreshComplaint: defineAction({ input: z.object({id:z.string().uuid()}), handler: ({id}) => api(`/api/v1/crm/cases/${id}/approve-fresh`, {method:"POST"}) }),
    publishComplaint: defineAction({ input: z.object({id:z.string().uuid()}), handler: ({id}) => api(`/api/v1/crm/cases/${id}/publish`, {method:"POST"}) }),
    publishComplaintBatch: defineAction({
      input: z.object({caseIds:z.array(z.string().uuid()).min(1).max(200)}),
      handler: ({caseIds}) => api("/api/v1/crm/cases/publication-batches", {method:"POST", body:JSON.stringify({case_ids:caseIds})}),
    }),
    backfillPaperlessCases: defineAction({ input: z.object({limit:z.number().int().positive().max(100000).optional()}), handler: ({limit}) => api("/api/v1/crm/cases/backfill-paperless", {method:"POST", body:JSON.stringify({limit})}) }),
    helpdeskHealth: defineAction({ input: z.object({}), handler: () => api("/api/v1/crm/helpdesk/health") }),
    helpdeskStatistics: defineAction({ input: z.object({}), handler: () => api("/api/v1/crm/helpdesk/statistics") }),
    helpdeskPreview: defineAction({
      input: z.object({ limit: z.number().int().min(1).max(1000).default(200) }),
      handler: ({ limit }) => api(`/api/v1/crm/helpdesk/preview?limit=${limit}`),
    }),
    bootstrapHelpdesk: defineAction({ input: z.object({}), handler: () => api("/api/v1/crm/helpdesk/bootstrap", { method: "POST" }) }),
    syncHelpdeskCase: defineAction({
      input: z.object({ id: z.string().uuid(), force: z.boolean().default(false) }),
      handler: ({ id, force }) => api(`/api/v1/crm/helpdesk/cases/${id}/sync?force=${force}`, { method: "POST" }),
    }),
    syncHelpdeskBatch: defineAction({
      input: z.object({ caseIds: z.array(z.string().uuid()).max(200).default([]), previewToken: z.string().min(20).optional(), limit: z.number().int().min(1).max(200).default(100), force: z.boolean().default(false) }),
      handler: ({ caseIds, previewToken, limit, force }) => api("/api/v1/crm/helpdesk/sync", { method: "POST", body: JSON.stringify({ case_ids: caseIds, preview_token: previewToken, limit, force }) }),
    }),
    bootstrapHelpdeskTeams: defineAction({ input: z.object({}), handler: () => api("/api/v1/crm/helpdesk/teams/bootstrap", { method: "POST" }) }),
    helpdeskRoutingStatistics: defineAction({ input: z.object({}), handler: () => api("/api/v1/crm/helpdesk/routing/statistics") }),
    helpdeskRoutingPreview: defineAction({
      input: z.object({ limit: z.number().int().min(1).max(500).default(200) }),
      handler: ({ limit }) => api(`/api/v1/crm/helpdesk/routing/preview?limit=${limit}`),
    }),
    applyHelpdeskRouting: defineAction({
      input: z.object({ previewToken: z.string().min(20), force: z.boolean().default(false) }),
      handler: ({ previewToken, force }) => api("/api/v1/crm/helpdesk/routing/apply", { method: "POST", body: JSON.stringify({ preview_token: previewToken, force }) }),
    }),
    helpdeskWorkflowStatistics: defineAction({ input: z.object({}), handler: () => api("/api/v1/crm/helpdesk/workflow/statistics") }),
    helpdeskWorkflowPreview: defineAction({
      input: z.object({ limit: z.number().int().min(1).max(500).default(200) }),
      handler: ({ limit }) => api(`/api/v1/crm/helpdesk/workflow/preview?limit=${limit}`),
    }),
    pullHelpdeskWorkflow: defineAction({
      input: z.object({ caseIds: z.array(z.string().uuid()).max(500).default([]), limit: z.number().int().min(1).max(500).default(200) }),
      handler: ({ caseIds, limit }) => api("/api/v1/crm/helpdesk/workflow/pull", { method: "POST", body: JSON.stringify({ case_ids: caseIds, limit }) }),
    }),
    pullHelpdeskCaseWorkflow: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/crm/helpdesk/cases/${id}/workflow/pull`, { method: "POST" }),
    }),
    helpdeskWorkflowAudit: defineAction({ input: z.object({ limit: z.number().int().min(1).max(500).default(200) }), handler: ({limit}) => api(`/api/v1/crm/helpdesk/workflow/audit?limit=${limit}`) }),
    helpdeskEvents: defineAction({ input: z.object({ limit: z.number().int().min(1).max(500).default(100) }), handler: ({limit}) => api(`/api/v1/crm/helpdesk/events?limit=${limit}`) }),
    knowledgeFacets: defineAction({ input: z.object({}), handler: () => api("/api/v1/crm/knowledge/facets") }),
    knowledgeStatistics: defineAction({
      input: z.object({
        category: z.string().default(""), subCategory: z.string().default(""), sourceSystem: z.string().default(""), search: z.string().default(""),
        replyScope: z.enum(["approved", "with_reply", "awaiting", "all"]).default("approved"), aiEligibleOnly: z.boolean().default(false),
        dateFrom: z.string().default(""), dateTo: z.string().default(""),
      }),
      handler: (input) => {
        const params = new URLSearchParams({ category: input.category, sub_category: input.subCategory, source_system: input.sourceSystem, search: input.search, reply_scope: input.replyScope, ai_eligible_only: String(input.aiEligibleOnly), date_from: input.dateFrom, date_to: input.dateTo });
        for (const [key, value] of [...params]) if (!value) params.delete(key);
        return api(`/api/v1/crm/knowledge/statistics?${params}`);
      },
    }),
    knowledgeRecords: defineAction({
      input: z.object({
        category: z.string().default(""), subCategory: z.string().default(""), sourceSystem: z.string().default(""), search: z.string().default(""),
        replyScope: z.enum(["approved", "with_reply", "awaiting", "all"]).default("approved"), aiEligibleOnly: z.boolean().default(false),
        dateFrom: z.string().default(""), dateTo: z.string().default(""), page: z.number().int().min(1).default(1), pageSize: z.number().int().min(1).max(200).default(25),
      }),
      handler: (input) => {
        const params = new URLSearchParams({ category: input.category, sub_category: input.subCategory, source_system: input.sourceSystem, search: input.search, reply_scope: input.replyScope, ai_eligible_only: String(input.aiEligibleOnly), date_from: input.dateFrom, date_to: input.dateTo, page: String(input.page), page_size: String(input.pageSize) });
        for (const [key, value] of [...params]) if (!value) params.delete(key);
        return api(`/api/v1/crm/knowledge/records?${params}`);
      },
    }),
    taxonomy: defineAction({
      input: z.object({ includeInactive: z.boolean().default(true) }),
      handler: ({ includeInactive }) => api(`/api/v1/crm/taxonomy?include_inactive=${includeInactive}`),
    }),
    createTaxonomyCategory: defineAction({
      input: z.object({
        name: z.string().min(1).max(140), description: z.string().default(""), displayOrder: z.number().int().min(0).max(100000).default(100),
        active: z.boolean().default(true), defaultAiEligible: z.boolean().default(false), replyGuidance: z.string().default(""), policyNotes: z.string().default(""),
      }),
      handler: (input) => api("/api/v1/crm/taxonomy/categories", { method: "POST", body: JSON.stringify({ name: input.name, description: input.description, display_order: input.displayOrder, active: input.active, default_ai_eligible: input.defaultAiEligible, reply_guidance: input.replyGuidance, policy_notes: input.policyNotes }) }),
    }),
    updateTaxonomyCategory: defineAction({
      input: z.object({
        id: z.string().uuid(), name: z.string().min(1).max(140), description: z.string().default(""), displayOrder: z.number().int().min(0).max(100000).default(100),
        active: z.boolean().default(true), defaultAiEligible: z.boolean().default(false), replyGuidance: z.string().default(""), policyNotes: z.string().default(""),
      }),
      handler: ({ id, ...input }) => api(`/api/v1/crm/taxonomy/categories/${id}`, { method: "PUT", body: JSON.stringify({ name: input.name, description: input.description, display_order: input.displayOrder, active: input.active, default_ai_eligible: input.defaultAiEligible, reply_guidance: input.replyGuidance, policy_notes: input.policyNotes }) }),
    }),
    setTaxonomyCategoryActive: defineAction({
      input: z.object({ id: z.string().uuid(), active: z.boolean() }),
      handler: ({ id, active }) => api(`/api/v1/crm/taxonomy/categories/${id}/active`, { method: "PATCH", body: JSON.stringify({ active }) }),
    }),
    mergeTaxonomyCategory: defineAction({
      input: z.object({ id: z.string().uuid(), targetId: z.string().uuid() }),
      handler: ({ id, targetId }) => api(`/api/v1/crm/taxonomy/categories/${id}/merge`, { method: "POST", body: JSON.stringify({ target_id: targetId }) }),
    }),
    syncTaxonomyCategory: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/crm/taxonomy/categories/${id}/sync`, { method: "POST" }),
    }),
    createTaxonomySubcategory: defineAction({
      input: z.object({ categoryId: z.string().uuid(), name: z.string().min(1).max(140), description: z.string().default(""), displayOrder: z.number().int().min(0).max(100000).default(100), active: z.boolean().default(true), replyGuidance: z.string().default(""), policyNotes: z.string().default("") }),
      handler: (input) => api("/api/v1/crm/taxonomy/subcategories", { method: "POST", body: JSON.stringify({ category_id: input.categoryId, name: input.name, description: input.description, display_order: input.displayOrder, active: input.active, reply_guidance: input.replyGuidance, policy_notes: input.policyNotes }) }),
    }),
    updateTaxonomySubcategory: defineAction({
      input: z.object({ id: z.string().uuid(), categoryId: z.string().uuid(), name: z.string().min(1).max(140), description: z.string().default(""), displayOrder: z.number().int().min(0).max(100000).default(100), active: z.boolean().default(true), replyGuidance: z.string().default(""), policyNotes: z.string().default("") }),
      handler: ({ id, ...input }) => api(`/api/v1/crm/taxonomy/subcategories/${id}`, { method: "PUT", body: JSON.stringify({ category_id: input.categoryId, name: input.name, description: input.description, display_order: input.displayOrder, active: input.active, reply_guidance: input.replyGuidance, policy_notes: input.policyNotes }) }),
    }),
    setTaxonomySubcategoryActive: defineAction({
      input: z.object({ id: z.string().uuid(), active: z.boolean() }),
      handler: ({ id, active }) => api(`/api/v1/crm/taxonomy/subcategories/${id}/active`, { method: "PATCH", body: JSON.stringify({ active }) }),
    }),
    mergeTaxonomySubcategory: defineAction({
      input: z.object({ id: z.string().uuid(), targetId: z.string().uuid() }),
      handler: ({ id, targetId }) => api(`/api/v1/crm/taxonomy/subcategories/${id}/merge`, { method: "POST", body: JSON.stringify({ target_id: targetId }) }),
    }),
    replyWorkspaceStatistics: defineAction({ input: z.object({}), handler: () => api("/api/v1/crm/reply-workspace/statistics") }),
    replyWorkspaceCases: defineAction({
      input: z.object({ categoryId: z.string().uuid().or(z.literal("")).default(""), subcategoryId: z.string().uuid().or(z.literal("")).default(""), replyStatus: z.string().default(""), sourceSystem: z.string().default(""), search: z.string().default(""), aiEligible: z.union([z.literal(""), z.literal("true"), z.literal("false")]).default(""), dateFrom: z.string().default(""), dateTo: z.string().default(""), page: z.number().int().min(1).default(1), pageSize: z.number().int().min(1).max(200).default(25) }),
      handler: (input) => {
        const params = new URLSearchParams({ category_id: input.categoryId, subcategory_id: input.subcategoryId, reply_status: input.replyStatus, source_system: input.sourceSystem, search: input.search, ai_eligible: input.aiEligible, date_from: input.dateFrom, date_to: input.dateTo, page: String(input.page), page_size: String(input.pageSize) });
        for (const [key, value] of [...params]) if (!value) params.delete(key);
        return api(`/api/v1/crm/reply-workspace/cases?${params}`);
      },
    }),
    replyWorkspaceCase: defineAction({ input: z.object({ id: z.string().uuid() }), handler: ({ id }) => api(`/api/v1/crm/reply-workspace/cases/${id}`) }),
    saveCaseClassification: defineAction({
      input: z.object({ id: z.string().uuid(), categoryId: z.string().uuid(), subcategoryId: z.string().uuid().or(z.literal("")).default(""), synchronize: z.boolean().default(true) }),
      handler: ({ id, categoryId, subcategoryId, synchronize }) => api(`/api/v1/crm/reply-workspace/cases/${id}/classification`, { method: "PUT", body: JSON.stringify({ category_id: categoryId, subcategory_id: subcategoryId || null, synchronize }) }),
    }),
    bulkClassifyCases: defineAction({
      input: z.object({ caseIds: z.array(z.string().uuid()).min(1).max(500), categoryId: z.string().uuid(), subcategoryId: z.string().uuid().or(z.literal("")).default(""), synchronize: z.boolean().default(true) }),
      handler: ({ caseIds, categoryId, subcategoryId, synchronize }) => api("/api/v1/crm/reply-workspace/classifications/bulk", { method: "POST", body: JSON.stringify({ case_ids: caseIds, category_id: categoryId, subcategory_id: subcategoryId || null, synchronize }) }),
    }),
    saveCaseReply: defineAction({
      input: z.object({ id: z.string().uuid(), inquiryFindings: z.string().default(""), schoolVersion: z.string().default(""), applicablePolicy: z.string().default(""), finalReply: z.string().default(""), replyStatus: z.enum(["Not Prepared", "Draft", "Pending Approval", "Approved", "Issued", "Rejected"]), disposalOutcome: z.string().default(""), aiEligible: z.boolean().default(false) }),
      handler: ({ id, ...input }) => api(`/api/v1/crm/reply-workspace/cases/${id}/reply`, { method: "PUT", body: JSON.stringify({ inquiry_findings: input.inquiryFindings, school_version: input.schoolVersion, applicable_policy: input.applicablePolicy, final_reply: input.finalReply, reply_status: input.replyStatus, disposal_outcome: input.disposalOutcome, ai_eligible: input.aiEligible }) }),
    }),
    sheets: defineAction({
      input: z.object({
        search: z.string().default(""),
        status: z.string().default(""),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(10),
      }),
      handler: (input) => {
        const params = new URLSearchParams({
          search: input.search,
          validation_status: input.status,
          page: String(input.page),
          page_size: String(input.pageSize),
        });
        return api(`/api/v1/crm/sheets?${params}`);
      },
    }),
    processSheet: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/crm/sheets/${id}/process`, { method: "POST" }),
    }),
    convertSheetToPdfs: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/crm/sheets/${id}/convert-to-pdfs`, { method: "POST" }),
    }),
    hardDeleteSheet: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/crm/sheets/${id}/hard`, { method: "DELETE" }),
    }),
    pdfBatches: defineAction({
      input: z.object({
        search: z.string().default(""),
        status: z.string().default(""),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(10),
      }),
      handler: (input) => {
        const params = new URLSearchParams({
          search: input.search,
          validation_status: input.status,
          page: String(input.page),
          page_size: String(input.pageSize),
        });
        return api(`/api/v1/crm/pdf-batches?${params}`);
      },
    }),
    processPdfBatch: defineAction({
      input: z.object({ id: z.string().uuid(), paperlessLimit: z.number().int().positive().max(10000).optional() }),
      handler: ({ id, paperlessLimit }) => api(`/api/v1/crm/pdf-batches/${id}/process`, {
        method: "POST",
        body: JSON.stringify(paperlessLimit ? { paperless_limit: paperlessLimit } : {}),
      }),
    }),
    hardDeletePdfBatch: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/crm/pdf-batches/${id}/hard`, { method: "DELETE" }),
    }),
    overview: defineAction({ handler: () => api("/api/v1/crm/overview") }),
  },
  whatsapp: {
    overview: defineAction({ handler: () => api("/api/v1/whatsapp/overview") }),
    connection: defineAction({ handler: () => api("/api/v1/whatsapp/connection") }),
    directorySummary: defineAction({ handler: () => api("/api/v1/whatsapp/directory/summary") }),
    configurationOptions: defineAction({ handler: () => api("/api/v1/whatsapp/configuration/options") }),
    reportTypes: defineAction({
      input: z.object({ applicationId: z.string().default("") }),
      handler: ({ applicationId }) => api(`/api/v1/whatsapp/report-types${applicationId ? `?application_id=${applicationId}` : ""}`),
    }),
    saveReportType: defineAction({
      input: z.object({
        id: z.string().uuid().optional(),
        application_id: z.string().uuid(),
        key: z.string().regex(/^[a-z0-9_]+$/),
        name: z.string().min(2),
        description: z.string().default(""),
        artifact_kind: z.string().default("document"),
        enabled: z.boolean().default(true),
      }),
      handler: (input) => api("/api/v1/whatsapp/report-types", { method: "POST", body: JSON.stringify(input) }),
    }),
    hardDeleteReportType: defineAction({ input: z.object({ id: z.string().uuid() }), handler: ({id}) => api(`/api/v1/whatsapp/report-types/${id}/hard`, {method:"DELETE"}) }),
    recipientScopes: defineAction({
      input: z.object({ applicationId: z.string().default(""), channel: z.enum(["individual", "group"]).optional() }),
      handler: (input) => {
        const params = new URLSearchParams();
        if (input.applicationId) params.set("application_id", input.applicationId);
        if (input.channel) params.set("channel", input.channel);
        return api(`/api/v1/whatsapp/recipient-scopes${params.size ? `?${params}` : ""}`);
      },
    }),
    saveRecipientScope: defineAction({
      input: z.object({
        id: z.string().uuid().optional(), application_id: z.string().uuid(), parent_id: z.string().uuid().optional(),
        channel: z.enum(["individual", "group"]), key: z.string().regex(/^[a-z0-9_]+$/), name: z.string().min(2),
        hierarchy_level: z.number().int().min(0).max(99), description: z.string().default(""), enabled: z.boolean(),
      }),
      handler: (input) => api("/api/v1/whatsapp/recipient-scopes", { method: "POST", body: JSON.stringify(input) }),
    }),
    hardDeleteRecipientScope: defineAction({ input: z.object({ id: z.string().uuid() }), handler: ({id}) => api(`/api/v1/whatsapp/recipient-scopes/${id}/hard`, {method:"DELETE"}) }),
    audiences: defineAction({
      input: z.object({
        applicationId: z.string().default(""),
        search: z.string().default(""),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(10),
      }),
      handler: (input) => {
        const params = new URLSearchParams({ search: input.search, page: String(input.page), page_size: String(input.pageSize) });
        if (input.applicationId) params.set("application_id", input.applicationId);
        return api(`/api/v1/whatsapp/audiences?${params}`);
      },
    }),
    audience: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/whatsapp/audiences/${id}`),
    }),
    saveAudience: defineAction({
      input: z.object({
        id: z.string().uuid().optional(),
        application_id: z.string().uuid(),
        key: z.string().regex(/^[a-z0-9_]+$/),
        name: z.string().min(2),
        description: z.string().default(""),
        enabled: z.boolean().default(true),
      }),
      handler: (input) => api("/api/v1/whatsapp/audiences", { method: "POST", body: JSON.stringify(input) }),
    }),
    hardDeleteAudience: defineAction({ input: z.object({ id: z.string().uuid() }), handler: ({id}) => api(`/api/v1/whatsapp/audiences/${id}/hard`, {method:"DELETE"}) }),
    audienceMembers: defineAction({
      input: z.object({ id: z.string().uuid(), page: z.number().int().positive().default(1), pageSize: z.number().int().min(1).max(100).default(10) }),
      handler: (input) => api(`/api/v1/whatsapp/audiences/${input.id}/members?page=${input.page}&page_size=${input.pageSize}`),
    }),
    addAudienceSource: defineAction({
      input: z.object({
        id: z.string().uuid(),
        source_type: z.literal("master_data_jurisdictions").default("master_data_jurisdictions"),
        recipient_role: z.enum(["aeo", "ddeo"]),
        wing_id: z.string().uuid(),
        route_scope_key: z.enum(["markaz", "tehsil"]),
        aggregate_by_recipient: z.boolean().default(true),
        enabled: z.boolean().default(true),
      }),
      handler: ({ id, ...input }) => api(`/api/v1/whatsapp/audiences/${id}/sources`, {
        method: "POST", body: JSON.stringify(input),
      }),
    }),
    addAudienceMember: defineAction({
      input: z.object({
        id: z.string().uuid(), target_type: z.enum(["group", "contact"]), target_id: z.string().uuid(),
        wing_id: z.string().uuid().optional(),
        route_scope_key: z.string().default(""), route_scope_value: z.string().default(""), route_scope_label: z.string().default(""),
      }),
      handler: ({ id, ...input }) => api(`/api/v1/whatsapp/audiences/${id}/members`, { method: "POST", body: JSON.stringify(input) }),
    }),
    updateAudienceMember: defineAction({
      input: z.object({
        audienceId: z.string().uuid(), memberId: z.string().uuid(), target_type: z.enum(["group", "contact"]), target_id: z.string().uuid(),
        wing_id: z.string().uuid().optional(),
        route_scope_key: z.string().default(""), route_scope_value: z.string().default(""), route_scope_label: z.string().default(""),
      }),
      handler: ({ audienceId, memberId, ...input }) => api(`/api/v1/whatsapp/audiences/${audienceId}/members/${memberId}`, { method: "PUT", body: JSON.stringify(input) }),
    }),
    setAudienceMemberEnabled: defineAction({
      input: z.object({ audienceId: z.string().uuid(), memberId: z.string().uuid(), enabled: z.boolean() }),
      handler: ({ audienceId, memberId, enabled }) => api(`/api/v1/whatsapp/audiences/${audienceId}/members/${memberId}/enabled`, { method: "PATCH", body: JSON.stringify({ enabled }) }),
    }),
    removeAudienceMember: defineAction({
      input: z.object({ audienceId: z.string().uuid(), memberId: z.string().uuid() }),
      handler: ({ audienceId, memberId }) => api(`/api/v1/whatsapp/audiences/${audienceId}/members/${memberId}`, { method: "DELETE" }),
    }),
    dispatchProfiles: defineAction({
      input: z.object({
        applicationId: z.string().default(""),
        search: z.string().default(""),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(10),
      }),
      handler: (input) => {
        const params = new URLSearchParams({ search: input.search, page: String(input.page), page_size: String(input.pageSize) });
        if (input.applicationId) params.set("application_id", input.applicationId);
        return api(`/api/v1/whatsapp/dispatch-profiles?${params}`);
      },
    }),
    saveDispatchProfile: defineAction({
      input: z.object({
        id: z.string().uuid().optional(), application_id: z.string().uuid(), key: z.string().regex(/^[a-z0-9_]+$/), name: z.string().min(2),
        report_type_id: z.string().uuid(), audience_id: z.string().uuid(), account_id: z.string().uuid(), template_id: z.string().uuid().optional(), wing_id: z.string().uuid().optional(),
        recipient_channel: z.enum(["individual", "group"]), recipient_scope_id: z.string().uuid().optional(), delivery_mode: z.enum(["groups", "individuals"]), delivery_granularity: z.enum(["recipient", "scope"]).default("recipient"), require_approval: z.boolean(), fallback_policy: z.enum(["none", "same_scope"]),
        max_retries: z.number().int().min(0).max(10), messages_per_minute: z.number().int().min(1).max(120), enabled: z.boolean(), notes: z.string().default(""),
        message_style: z.enum(["summary", "detailed"]).default("summary"), attachment_mode: z.enum(["none", "image", "excel", "image_excel"]).default("image_excel"), image_content: z.enum(["summary", "details"]).default("details"),
      }),
      handler: (input) => api("/api/v1/whatsapp/dispatch-profiles", { method: "POST", body: JSON.stringify(input) }),
    }),
    setupReportingRoute: defineAction({
      input: z.object({
        profile_id: z.string().uuid().optional(), application_id: z.string().uuid().optional(), report_type_id: z.string().uuid().optional(),
        wing_id: z.string().uuid().optional(), recipient_scope_id: z.string().uuid().optional(), account_id: z.string().uuid().optional(),
        directory_group_id: z.string().uuid(), route_scope_value: z.string().min(1), route_scope_label: z.string().min(1),
        template_id: z.string().uuid().optional(), create_recommended_template: z.boolean().default(true),
        audience_name: z.string().default(""), profile_name: z.string().default(""), template_name: z.string().default(""),
        template_body: z.string().min(1).default("{{report_body}}"), require_approval: z.boolean().default(true),
        max_retries: z.number().int().min(0).max(10).default(5), messages_per_minute: z.number().int().min(1).max(120).default(20),
        message_style: z.enum(["summary", "detailed"]).default("summary"), attachment_mode: z.enum(["none", "image", "excel", "image_excel"]).default("image_excel"), image_content: z.enum(["summary", "details"]).default("details"),
      }),
      handler: (input) => api("/api/v1/whatsapp/reporting-routes/setup", { method: "POST", body: JSON.stringify(input) }),
    }),
    archiveDispatchProfile: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/whatsapp/dispatch-profiles/${id}`, { method: "DELETE" }),
    }),
    hardDeleteDispatchProfile: defineAction({ input: z.object({ id: z.string().uuid() }), handler: ({id}) => api(`/api/v1/whatsapp/dispatch-profiles/${id}/hard`, {method:"DELETE"}) }),
    previewOptions: defineAction({ handler: () => api("/api/v1/whatsapp/previews/options") }),
    createPreview: defineAction({
      input: z.object({ source_job_id: z.string().uuid(), dispatch_profile_id: z.string().uuid() }),
      handler: (input) => api("/api/v1/whatsapp/previews", {
        method: "POST",
        body: JSON.stringify(input),
      }),
    }),
    createPreviewsBulk: defineAction({
      input: z.object({
        source_job_id: z.string().uuid(),
        dispatch_profile_ids: z.array(z.string().uuid()).min(1).max(50),
      }),
      handler: (input) => api("/api/v1/whatsapp/previews/bulk", {
        method: "POST",
        body: JSON.stringify(input),
      }),
    }),
    job: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/jobs/${id}`),
    }),
    jobLogs: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/jobs/${id}/logs`),
    }),
    jobArtifacts: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/jobs/${id}/artifacts`),
    }),
    inboundStatus: defineAction({
      handler: () => api("/api/v1/whatsapp/inbound/status"),
    }),
    inboundHistoryBridgeStatus: defineAction({
      input: z.object({ contactId: z.string().uuid() }),
      handler: ({ contactId }) => {
        const params = new URLSearchParams({ contact_id: contactId });
        return api(`/api/v1/whatsapp/inbound/history/bridge/status?${params}`);
      },
    }),
    requestInboundHistory: defineAction({
      input: z.object({
        contact_id: z.string().uuid(),
        count: z.number().int().min(1).max(5000).default(50),
        all_history: z.boolean().default(false),
      }),
      handler: (input) => api("/api/v1/whatsapp/inbound/history/request", {
        method: "POST",
        body: JSON.stringify(input),
      }),
    }),
    inboundHistoryRequests: defineAction({
      input: z.object({
        contactId: z.string().uuid().optional(),
        limit: z.number().int().min(1).max(100).default(10),
      }),
      handler: (input) => {
        const params = new URLSearchParams({ limit: String(input.limit) });
        if (input.contactId) params.set("contact_id", input.contactId);
        return api(`/api/v1/whatsapp/inbound/history/requests?${params}`);
      },
    }),
    inboundHistoryRequest: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/whatsapp/inbound/history/requests/${id}`),
    }),
    inboundBatches: defineAction({
      input: z.object({
        contactId: z.string().uuid().optional(),
        status: z.string().optional(),
        search: z.string().optional(),
        limit: z.number().int().min(1).max(200).default(50),
      }),
      handler: (input) => {
        const params = new URLSearchParams({ limit: String(input.limit) });
        if (input.contactId) params.set("contact_id", input.contactId);
        if (input.status) params.set("status", input.status);
        if (input.search) params.set("search", input.search);
        return api(`/api/v1/whatsapp/inbound/batches?${params}`);
      },
    }),
    inboundContactWorkspace: defineAction({
      input: z.object({
        contactId: z.string().uuid(),
        category: z.enum(["image", "pdf", "spreadsheet"]).optional(),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(50),
      }),
      handler: ({ contactId, category, page, pageSize }) => {
        const params = new URLSearchParams({ page: String(page), page_size: String(pageSize) });
        if (category) params.set("category", category);
        return api(`/api/v1/whatsapp/inbound/contacts/${contactId}/workspace?${params}`);
      },
    }),
    inboundBatch: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/whatsapp/inbound/batches/${id}`),
    }),
    inboundBatchEvents: defineAction({
      input: z.object({ id: z.string().uuid(), after: z.string().optional(), limit: z.number().int().min(1).max(1000).default(250) }),
      handler: ({ id, after, limit }) => {
        const params = new URLSearchParams({ limit: String(limit) });
        if (after) params.set("after", after);
        return api(`/api/v1/whatsapp/inbound/batches/${id}/events?${params}`);
      },
    }),
    retryInboundBatchStorage: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/whatsapp/inbound/batches/${id}/retry-storage`, { method: "POST" }),
    }),
    inboundStorageStatus: defineAction({
      handler: () => api("/api/v1/whatsapp/inbound/storage/status"),
    }),
    createInboundProcessingRun: defineAction({
      input: z.object({
        batch_id: z.string().uuid(),
        paperless_check: z.boolean().default(true),
      }),
      handler: (input) => api("/api/v1/whatsapp/inbound/processing-runs", {
        method: "POST",
        body: JSON.stringify(input),
      }),
    }),
    inboundProcessingRuns: defineAction({
      input: z.object({
        status: z.string().optional(),
        category: z.string().optional(),
        search: z.string().optional(),
        limit: z.number().int().min(1).max(200).default(50),
      }),
      handler: (input) => {
        const params = new URLSearchParams({ limit: String(input.limit) });
        if (input.status) params.set("status", input.status);
        if (input.category) params.set("category", input.category);
        if (input.search) params.set("search", input.search);
        return api(`/api/v1/whatsapp/inbound/processing-runs?${params}`);
      },
    }),
    inboundProcessingRun: defineAction({
      input: z.object({
        id: z.string().uuid(),
        category: z.string().optional(),
        reviewStatus: z.string().optional(),
      }),
      handler: ({ id, category, reviewStatus }) => {
        const params = new URLSearchParams();
        if (category) params.set("category", category);
        if (reviewStatus) params.set("review_status", reviewStatus);
        const suffix = params.toString() ? `?${params}` : "";
        return api(`/api/v1/whatsapp/inbound/processing-runs/${id}${suffix}`);
      },
    }),
    inboundProcessingEvents: defineAction({
      input: z.object({ id: z.string().uuid(), after: z.string().optional(), limit: z.number().int().min(1).max(2000).default(500) }),
      handler: ({ id, after, limit }) => {
        const params = new URLSearchParams({ limit: String(limit) });
        if (after) params.set("after", after);
        return api(`/api/v1/whatsapp/inbound/processing-runs/${id}/events?${params}`);
      },
    }),
    reviewInboundComplaintGroup: defineAction({
      input: z.object({
        runId: z.string().uuid(),
        complaintNumber: z.string().min(1).max(40),
        decision: z.enum(["approved", "rejected"]),
        reviewed_by: z.string().min(1).max(100).default("web-operator"),
        note: z.string().max(4000).nullable().optional(),
      }),
      handler: ({ runId, complaintNumber, ...body }) => api(
        `/api/v1/whatsapp/inbound/processing-runs/${runId}/complaint-groups/${encodeURIComponent(complaintNumber)}/review`,
        { method: "POST", body: JSON.stringify(body) },
      ),
    }),
    batchApproveInboundComplaintGroups: defineAction({
      input: z.object({
        runId: z.string().uuid(),
        complaintNumbers: z.array(z.string().min(1).max(40)).min(1).max(200),
        reviewed_by: z.string().min(1).max(100).default("web-operator"),
        note: z.string().max(4000).nullable().optional(),
      }),
      handler: ({ runId, complaintNumbers, ...body }) => api(
        `/api/v1/whatsapp/inbound/processing-runs/${runId}/complaint-group-approvals`,
        {
          method: "POST",
          body: JSON.stringify({ ...body, complaint_numbers: complaintNumbers }),
        },
      ),
    }),
    reviewInboundProcessingItem: defineAction({
      input: z.object({
        id: z.string().uuid(),
        decision: z.enum(["pending", "approved", "rejected", "deferred"]),
        reviewed_by: z.string().min(1).max(100).default("web-operator"),
        note: z.string().max(4000).nullable().optional(),
        category: z.string().max(80).nullable().optional(),
        complaint_number: z.string().max(40).nullable().optional(),
      }),
      handler: ({ id, ...body }) => api(`/api/v1/whatsapp/inbound/processing-items/${id}/review`, {
        method: "POST",
        body: JSON.stringify(body),
      }),
    }),
    inboundExportPreview: defineAction({
      input: z.object({
        contact_id: z.string().uuid(),
        date_from: z.string().datetime().nullable().optional(),
        date_to: z.string().datetime().nullable().optional(),
        chat_scope: z.enum(["direct", "direct_and_groups"]).default("direct"),
        media_types: z.array(z.enum(["image", "pdf", "spreadsheet"])).min(1),
      }),
      handler: (input) => api("/api/v1/whatsapp/inbound/exports/preview", {
        method: "POST",
        body: JSON.stringify(input),
      }),
    }),
    createInboundExport: defineAction({
      input: z.object({
        contact_id: z.string().uuid(),
        date_from: z.string().datetime().nullable().optional(),
        date_to: z.string().datetime().nullable().optional(),
        chat_scope: z.enum(["direct", "direct_and_groups"]).default("direct"),
        media_types: z.array(z.enum(["image", "pdf", "spreadsheet"])).min(1),
        requested_by: z.string().min(1).max(100).default("web-operator"),
      }),
      handler: (input) => api("/api/v1/whatsapp/inbound/exports", {
        method: "POST",
        body: JSON.stringify(input),
      }),
    }),
    inboundExports: defineAction({
      input: z.object({
        contactId: z.string().uuid().optional(),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(10),
      }),
      handler: (input) => {
        const params = new URLSearchParams({
          page: String(input.page),
          page_size: String(input.pageSize),
        });
        if (input.contactId) params.set("contact_id", input.contactId);
        return api(`/api/v1/whatsapp/inbound/exports?${params}`);
      },
    }),
    inboundExport: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/whatsapp/inbound/exports/${id}`),
    }),
    previews: defineAction({
      input: z.object({
        search: z.string().default(""),
        status: z.string().default(""),
        view: z.enum(["pending", "sending", "history", "all"]).default("pending"),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(10),
      }),
      handler: (input) => {
        const params = new URLSearchParams({
          search: input.search,
          preview_status: input.status,
          view: input.view,
          page: String(input.page),
          page_size: String(input.pageSize),
        });
        return api(`/api/v1/whatsapp/previews?${params}`);
      },
    }),
    previewSelection: defineAction({
      input: z.object({ search: z.string().default(""), status: z.string().default(""), view: z.enum(["pending", "sending", "history", "all"]).default("pending") }),
      handler: (input) => {
        const params = new URLSearchParams({ search: input.search, preview_status: input.status, view: input.view });
        return api(`/api/v1/whatsapp/previews/selection?${params}`);
      },
    }),
    bulkApprovePreviews: defineAction({
      input: z.object({
        preview_ids: z.array(z.string().uuid()).min(1).max(50),
        acknowledge_warnings: z.boolean().default(false),
        acknowledge_exclusions: z.boolean().default(false),
        approved_by: z.string().min(1).max(100).default("web-operator"),
      }),
      handler: (input) => api("/api/v1/whatsapp/preview-bulk/approve", {
        method: "POST",
        body: JSON.stringify(input),
      }),
    }),
    bulkDeletePreviews: defineAction({
      input: z.object({ preview_ids: z.array(z.string().uuid()).min(1).max(100) }),
      handler: (input) => api("/api/v1/whatsapp/preview-bulk", {
        method: "DELETE",
        body: JSON.stringify(input),
      }),
    }),
    preview: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/whatsapp/previews/${id}`),
    }),
    approvePreview: defineAction({
      input: z.object({
        id: z.string().uuid(),
        acknowledge_warnings: z.boolean().default(false),
        acknowledge_exclusions: z.boolean().default(false),
        approved_by: z.string().min(1).max(100).default("web-operator"),
      }),
      handler: ({ id, ...input }) => api(`/api/v1/whatsapp/previews/${id}/approve`, {
        method: "POST",
        body: JSON.stringify(input),
      }),
    }),
    hardDeletePreview: defineAction({ input: z.object({ id: z.string().uuid() }), handler: ({id}) => api(`/api/v1/whatsapp/previews/${id}`, {method:"DELETE"}) }),
    previewDeliveries: defineAction({
      input: z.object({
        id: z.string().uuid(),
        search: z.string().default(""),
        status: z.string().default(""),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(10),
      }),
      handler: (input) => {
        const params = new URLSearchParams({
          search: input.search,
          delivery_status: input.status,
          page: String(input.page),
          page_size: String(input.pageSize),
        });
        return api(`/api/v1/whatsapp/previews/${input.id}/deliveries?${params}`);
      },
    }),
    previewIssues: defineAction({
      input: z.object({
        id: z.string().uuid(),
        severity: z.string().default(""),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(10),
      }),
      handler: (input) => {
        const params = new URLSearchParams({
          severity: input.severity,
          page: String(input.page),
          page_size: String(input.pageSize),
        });
        return api(`/api/v1/whatsapp/previews/${input.id}/issues?${params}`);
      },
    }),
    previewArtifacts: defineAction({
      input: z.object({
        id: z.string().uuid(),
        role: z.string().default(""),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(10),
      }),
      handler: (input) => {
        const params = new URLSearchParams({
          role: input.role,
          page: String(input.page),
          page_size: String(input.pageSize),
        });
        return api(`/api/v1/whatsapp/previews/${input.id}/artifacts?${params}`);
      },
    }),
    syncDirectory: defineAction({
      handler: () => api("/api/v1/whatsapp/directory/sync", { method: "POST" }),
    }),
    directoryGroups: defineAction({
      input: z.object({
        search: z.string().default(""),
        availability: z.enum(["all", "available", "unavailable"]).default("all"),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(10),
      }),
      handler: (input) => {
        const params = new URLSearchParams({
          search: input.search,
          availability: input.availability,
          page: String(input.page),
          page_size: String(input.pageSize),
        });
        return api(`/api/v1/whatsapp/directory/groups?${params}`);
      },
    }),
    directoryGroup: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/whatsapp/directory/groups/${id}`),
    }),
    syncGroupMembers: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/whatsapp/directory/groups/${id}/members/sync`, { method: "POST" }),
    }),
    groupMembers: defineAction({
      input: z.object({
        id: z.string().uuid(),
        search: z.string().default(""),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(10),
      }),
      handler: (input) => {
        const params = new URLSearchParams({
          search: input.search,
          page: String(input.page),
          page_size: String(input.pageSize),
        });
        return api(`/api/v1/whatsapp/directory/groups/${input.id}/members?${params}`);
      },
    }),
    directoryContacts: defineAction({
      input: z.object({
        search: z.string().default(""),
        tokenStatus: z.string().default(""),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(10),
      }),
      handler: (input) => {
        const params = new URLSearchParams({
          search: input.search,
          token_status: input.tokenStatus,
          page: String(input.page),
          page_size: String(input.pageSize),
        });
        return api(`/api/v1/whatsapp/directory/contacts?${params}`);
      },
    }),
    refreshContactToken: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/whatsapp/directory/contacts/${id}/token/refresh`, { method: "POST" }),
    }),
    saveMasterContactName: defineAction({
      input: z.object({ id: z.string().uuid(), name: z.string().trim().min(1).max(200) }),
      handler: ({ id, name }) => api(`/api/v1/whatsapp/directory/contacts/${id}/master-contact`, {
        method: "PUT",
        body: JSON.stringify({ name }),
      }),
    }),
    contactLinks: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/whatsapp/directory/contacts/${id}/links`),
    }),
    contactLinkTargets: defineAction({
      input: z.object({
        contactId: z.string().uuid(),
        entityType: z.enum(["officer", "school_head"]),
        search: z.string().default(""),
      }),
      handler: (input) => {
        const params = new URLSearchParams({
          contact_id: input.contactId,
          entity_type: input.entityType,
          search: input.search,
        });
        return api(`/api/v1/whatsapp/directory/contact-link-targets?${params}`);
      },
    }),
    saveContactLink: defineAction({
      input: z.object({
        contactId: z.string().uuid(),
        entity_type: z.enum(["officer", "school_head"]),
        entity_id: z.string().uuid(),
      }),
      handler: ({ contactId, ...input }) => api(`/api/v1/whatsapp/directory/contacts/${contactId}/links`, {
        method: "POST",
        body: JSON.stringify(input),
      }),
    }),
    removeContactLink: defineAction({
      input: z.object({ contactId: z.string().uuid(), linkId: z.string().uuid() }),
      handler: ({ contactId, linkId }) => api(`/api/v1/whatsapp/directory/contacts/${contactId}/links/${linkId}`, { method: "DELETE" }),
    }),
    recipients: defineAction({
      input: z.object({
        search: z.string().default(""),
        recipientType: z.enum(["all", "officer", "school_head"]).default("all"),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(10),
      }),
      handler: (input) => {
        const params = new URLSearchParams({
          search: input.search,
          recipient_type: input.recipientType,
          page: String(input.page),
          page_size: String(input.pageSize),
        });
        return api(`/api/v1/whatsapp/recipients?${params}`);
      },
    }),
    groups: defineAction({ handler: () => api("/api/v1/whatsapp/groups") }),
    saveGroup: defineAction({
      input: z.object({
        id: z.string().optional(),
        directory_group_id: z.string().uuid().optional(),
        wing_id: z.string().uuid(),
        name: z.string().min(1),
        jid: z.string().min(10),
        purpose: z.string().default("reports"),
        notes: z.string().default(""),
        enabled: z.boolean().default(true),
      }),
      handler: (input) => api("/api/v1/whatsapp/groups", {
        method: "POST",
        body: JSON.stringify(input),
      }),
    }),
    archiveGroup: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/whatsapp/groups/${id}`, { method: "DELETE" }),
    }),
    hardDeleteGroup: defineAction({ input: z.object({ id: z.string().uuid() }), handler: ({id}) => api(`/api/v1/whatsapp/groups/${id}/hard`, {method:"DELETE"}) }),
    deliveries: defineAction({
      input: z.object({
        status: z.string().default(""),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(10),
      }),
      handler: (input) => {
        const params = new URLSearchParams({
          status: input.status,
          page: String(input.page),
          page_size: String(input.pageSize),
        });
        return api(`/api/v1/whatsapp/deliveries?${params}`);
      },
    }),
    templates: defineAction({ handler: () => api("/api/v1/whatsapp/templates") }),
    saveTemplate: defineAction({
      input: z.object({
        id: z.string().optional(),
        application_id: z.string().uuid().optional(),
        report_type_id: z.string().uuid().optional(),
        recipient_scope_id: z.string().uuid().optional(),
        recipient_channel: z.enum(["individual", "group", "any"]),
        key: z.string().min(1),
        name: z.string().min(1),
        category: z.enum(["report", "system", "escalation", "zero_result_acknowledgement"]).default("report"),
        body: z.string().min(1),
        enabled: z.boolean().default(true),
      }),
      handler: (input) => api("/api/v1/whatsapp/templates", {
        method: "POST",
        body: JSON.stringify(input),
      }),
    }),
    hardDeleteTemplate: defineAction({ input: z.object({ id: z.string().uuid() }), handler: ({id}) => api(`/api/v1/whatsapp/templates/${id}/hard`, {method:"DELETE"}) }),
    settings: defineAction({ handler: () => api("/api/v1/whatsapp/settings") }),
    updateSettings: defineAction({
      input: z.object({
        send_delay_ms: z.number().int().min(0).max(60000),
        acknowledgement_timeout_seconds: z.number().int().min(5).max(180),
        max_retries: z.number().int().min(1).max(10),
        require_preview: z.boolean(),
        live_delivery_enabled: z.boolean(),
      }),
      handler: (input) => api("/api/v1/whatsapp/settings", {
        method: "PUT",
        body: JSON.stringify(input),
      }),
    }),
    activity: defineAction({
      input: z.object({
        level: z.string().default(""),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(10),
      }),
      handler: (input) => {
        const params = new URLSearchParams({
          level: input.level,
          page: String(input.page),
          page_size: String(input.pageSize),
        });
        return api(`/api/v1/whatsapp/activity?${params}`);
      },
    }),
    sendTestMessage: defineAction({
      input: z.object({
        target: z.string().min(10),
        recipient_name: z.string().default("Test recipient"),
        message: z.string().min(1).max(3500),
      }),
      handler: (input) => api("/api/v1/whatsapp/test-message", {
        method: "POST",
        body: JSON.stringify(input),
      }),
    }),
  },
  masterData: {
    dashboard: defineAction({ handler: () => api("/api/v1/master-data/dashboard") }),
    hierarchy: defineAction({
      input: z.object({
        includeInactive: z.boolean().default(false),
        districtId: z.string().uuid().optional(),
        wingId: z.string().uuid().optional(),
      }),
      handler: ({ includeInactive, districtId, wingId }) => {
        const params = new URLSearchParams({ include_inactive: String(includeInactive) });
        if (districtId) params.set("district_id", districtId);
        if (wingId) params.set("wing_id", wingId);
        return api(`/api/v1/master-data/hierarchy?${params}`);
      },
    }),
    markazes: defineAction({
      input: z.object({
        search: z.string().default(""), wingId: z.string().uuid().optional(),
        tehsilId: z.string().uuid().optional(),
        coverage: z.enum(["all", "assigned", "unassigned"]).default("all"),
        includeInactive: z.boolean().default(false),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(20),
      }),
      handler: (input) => {
        const params = new URLSearchParams({
          search: input.search, coverage: input.coverage,
          include_inactive: String(input.includeInactive),
          page: String(input.page), page_size: String(input.pageSize),
        });
        if (input.wingId) params.set("wing_id", input.wingId);
        if (input.tehsilId) params.set("tehsil_id", input.tehsilId);
        return api(`/api/v1/master-data/markazes?${params}`);
      },
    }),
    options: defineAction({ handler: () => api("/api/v1/master-data/options") }),
    schools: defineAction({
      input: z.object({
        search: z.string().default(""),
        tehsilRef: z.string().default(""),
        markazRef: z.string().default(""),
        active: z.boolean().default(true),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(10),
      }),
      handler: (input) => {
        const params = new URLSearchParams({
          search: input.search,
          tehsil_ref: input.tehsilRef,
          markaz_ref: input.markazRef,
          active: String(input.active),
          page: String(input.page),
          page_size: String(input.pageSize),
        });
        return api(`/api/v1/master-data/schools?${params}`);
      },
    }),
    school: defineAction({
      input: z.object({ id: z.string().min(1) }),
      handler: ({ id }) => api(`/api/v1/master-data/schools/${id}`),
    }),
    saveSchool: defineAction({
      input: schoolInput,
      handler: ({ id, ...school }) => api(
        id ? `/api/v1/master-data/schools/${id}` : "/api/v1/master-data/schools",
        { method: id ? "PUT" : "POST", body: JSON.stringify(school) },
      ),
    }),
    setSchoolActive: defineAction({
      input: z.object({ id: z.string(), active: z.boolean() }),
      handler: ({ id, active }) => api(
        active ? `/api/v1/master-data/schools/${id}/restore` : `/api/v1/master-data/schools/${id}`,
        { method: active ? "POST" : "DELETE" },
      ),
    }),
    hardDeleteSchool: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/master-data/schools/${id}/hard`, { method: "DELETE" }),
    }),
    officers: defineAction({
      input: z.object({
        role: z.enum(["aeo", "ddeo"]),
        search: z.string().default(""),
        active: z.boolean().default(true),
      }),
      handler: (input) => {
        const params = new URLSearchParams({ role: input.role, search: input.search, active: String(input.active) });
        return api(`/api/v1/master-data/officers?${params}`);
      },
    }),
    saveOfficer: defineAction({
      input: officerInput,
      handler: ({ id, ...officer }) => api(
        id ? `/api/v1/master-data/officers/${officer.role}/${id}` : "/api/v1/master-data/officers",
        { method: id ? "PUT" : "POST", body: JSON.stringify(officer) },
      ),
    }),
    setOfficerActive: defineAction({
      input: z.object({ id: z.string(), role: z.enum(["aeo", "ddeo"]), active: z.boolean() }),
      handler: ({ id, role, active }) => api(
        active ? `/api/v1/master-data/officers/${role}/${id}/restore` : `/api/v1/master-data/officers/${role}/${id}`,
        { method: active ? "POST" : "DELETE" },
      ),
    }),
    hardDeleteOfficer: defineAction({
      input: z.object({ id: z.string().uuid(), role: z.enum(["aeo", "ddeo"]) }),
      handler: ({ id, role }) => api(`/api/v1/master-data/officers/${role}/${id}/hard`, { method: "DELETE" }),
    }),
    heads: defineAction({
      input: z.object({
        search: z.string().default(""),
        missingOnly: z.boolean().default(false),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(10),
      }),
      handler: (input) => {
        const params = new URLSearchParams({
          search: input.search,
          missing_only: String(input.missingOnly),
          page: String(input.page),
          page_size: String(input.pageSize),
        });
        return api(`/api/v1/master-data/heads?${params}`);
      },
    }),
    jurisdictions: defineAction({
      input: z.object({ active: z.boolean().default(true), wingRef: z.string().default("") }),
      handler: (input) => {
        const params = new URLSearchParams({ active: String(input.active), wing_ref: input.wingRef });
        return api(`/api/v1/master-data/jurisdictions?${params}`);
      },
    }),
    assignJurisdiction: defineAction({
      input: z.object({
        role: z.enum(["aeo", "ddeo"]),
        officer_ref: z.string().uuid(),
        scope_ref: z.string().uuid(),
        make_primary: z.boolean().default(false),
        replace_conflicts: z.boolean().default(false),
        notes: z.string().default(""),
      }),
      handler: (input) => api("/api/v1/master-data/jurisdictions", {
        method: "POST", body: JSON.stringify(input),
      }),
    }),
    endJurisdiction: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/master-data/jurisdictions/${id}`, { method: "DELETE" }),
    }),
    makePrimaryJurisdiction: defineAction({
      input: z.object({ id: z.string().uuid() }),
      handler: ({ id }) => api(`/api/v1/master-data/jurisdictions/${id}/make-primary`, { method: "POST" }),
    }),
    history: defineAction({
      input: z.object({ limit: z.number().int().min(1).max(500).default(200) }),
      handler: ({ limit }) => api(`/api/v1/master-data/history?limit=${limit}`),
    }),
  },
};
