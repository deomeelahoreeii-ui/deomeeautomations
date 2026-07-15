export function stack(primary: unknown, secondary: unknown = ""): DocumentFragment {
  const fragment = document.createDocumentFragment();
  const strong = document.createElement("strong");
  strong.textContent = String(primary || "—");
  fragment.append(strong);
  if (secondary !== "") {
    const small = document.createElement("small");
    small.textContent = String(secondary || "—");
    fragment.append(small);
  }
  return fragment;
}

export function badge(label: unknown, tone = ""): HTMLSpanElement {
  const element = document.createElement("span");
  element.className = `status-chip ${tone}`.trim();
  element.textContent = String(label);
  return element;
}

export function actionButtons(
  actions: Array<{ label: string; tone?: string; onClick: () => void }>,
): HTMLDivElement {
  const container = document.createElement("div");
  container.className = "table-actions";
  for (const action of actions) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `table-action ${action.tone || ""}`.trim();
    button.textContent = action.label;
    button.addEventListener("click", action.onClick);
    container.append(button);
  }
  return container;
}
