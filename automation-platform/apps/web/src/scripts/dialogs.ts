const focusOrigins = new WeakMap<HTMLDialogElement, HTMLElement>();

function resolveDialog(target: string | HTMLDialogElement): HTMLDialogElement {
  const dialog = typeof target === "string"
    ? document.getElementById(target)
    : target;
  if (!(dialog instanceof HTMLDialogElement)) {
    throw new TypeError(`Dialog ${String(target)} was not found.`);
  }
  return dialog;
}

function firstFocusable(dialog: HTMLDialogElement): HTMLElement | null {
  return dialog.querySelector<HTMLElement>(
    "[autofocus], input:not([type='hidden']):not([disabled]), select:not([disabled]), textarea:not([disabled]), button:not([disabled]), a[href]",
  );
}

export function openDialog(target: string | HTMLDialogElement): HTMLDialogElement {
  const dialog = resolveDialog(target);
  const active = document.activeElement;
  if (active instanceof HTMLElement) focusOrigins.set(dialog, active);
  if (!dialog.open) dialog.showModal();
  window.requestAnimationFrame(() => firstFocusable(dialog)?.focus({ preventScroll: true }));
  return dialog;
}

export function closeDialog(
  target: string | HTMLDialogElement,
  returnValue = "cancel",
): void {
  const dialog = resolveDialog(target);
  if (dialog.open) dialog.close(returnValue);
}

export function setDialogBusy(
  target: string | HTMLDialogElement,
  busy: boolean,
): void {
  const dialog = resolveDialog(target);
  dialog.dataset.busy = busy ? "true" : "false";
  dialog.setAttribute("aria-busy", busy ? "true" : "false");
  dialog.querySelectorAll<HTMLButtonElement>("[data-dialog-close]").forEach((button) => {
    button.disabled = busy;
  });
}

export function wireDialogs(root: ParentNode = document): void {
  root.querySelectorAll<HTMLElement>("[data-dialog-open]").forEach((trigger) => {
    if (trigger.dataset.dialogTriggerWired === "true") return;
    trigger.dataset.dialogTriggerWired = "true";
    trigger.addEventListener("click", () => {
      const id = trigger.dataset.dialogOpen;
      if (id) openDialog(id);
    });
  });

  root.querySelectorAll<HTMLButtonElement>("[data-dialog-close]").forEach((button) => {
    if (button.dataset.dialogCloseWired === "true") return;
    button.dataset.dialogCloseWired = "true";
    button.addEventListener("click", () => {
      const dialog = button.closest("dialog");
      if (dialog instanceof HTMLDialogElement) closeDialog(dialog);
    });
  });

  root.querySelectorAll<HTMLDialogElement>("dialog.crm-modal").forEach((dialog) => {
    if (dialog.dataset.dialogWired === "true") return;
    dialog.dataset.dialogWired = "true";

    dialog.addEventListener("click", (event) => {
      if (event.target === dialog && dialog.dataset.busy !== "true") {
        closeDialog(dialog);
      }
    });

    dialog.addEventListener("cancel", (event) => {
      if (dialog.dataset.busy === "true") event.preventDefault();
    });

    dialog.addEventListener("close", () => {
      const origin = focusOrigins.get(dialog);
      focusOrigins.delete(dialog);
      if (origin?.isConnected) origin.focus({ preventScroll: true });
    });
  });
}
