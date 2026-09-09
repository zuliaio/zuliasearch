package io.zulia.signals.model;

/** Open vocabulary, apps can add their own. */
public interface Actions {

	// session. HEARTBEAT keeps a session alive if logout is not reliable
	String LOGIN = "login";
	String LOGOUT = "logout";
	String HEARTBEAT = "heartbeat";

	// navigation. VISIT enters a page or workspace, VIEW opens one record.
	String VISIT = "visit";
	String VIEW = "view";

	String CREATE = "create";
	String UPDATE = "update";
	String DELETE = "delete";
	String UPLOAD = "upload";

	// curation. ANNOTATE covers coding, tagging, and labeling.
	String ANNOTATE = "annotate";
	String ASSIGN = "assign";

	String SEARCH = "search";
	String CLICK = "click";
	String EXPORT = "export";
	String SAVE = "save";
}
