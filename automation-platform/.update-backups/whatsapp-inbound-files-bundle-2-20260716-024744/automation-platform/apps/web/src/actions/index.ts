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
  active: z.boolean().default(true),
});

export const server = {
  antidengue: {
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
        recipient_channel: z.enum(["individual", "group"]), recipient_scope_id: z.string().uuid().optional(), delivery_mode: z.enum(["groups", "individuals"]), require_approval: z.boolean(), fallback_policy: z.enum(["none", "same_scope"]),
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
        category: z.string().default("report"),
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
    options: defineAction({ handler: () => api("/api/v1/master-data/options") }),
    schools: defineAction({
      input: z.object({
        search: z.string().default(""),
        tehsilRef: z.string().default(""),
        active: z.boolean().default(true),
        page: z.number().int().positive().default(1),
        pageSize: z.number().int().min(1).max(100).default(10),
      }),
      handler: (input) => {
        const params = new URLSearchParams({
          search: input.search,
          tehsil_ref: input.tehsilRef,
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
    history: defineAction({
      input: z.object({ limit: z.number().int().min(1).max(500).default(200) }),
      handler: ({ limit }) => api(`/api/v1/master-data/history?limit=${limit}`),
    }),
  },
};
