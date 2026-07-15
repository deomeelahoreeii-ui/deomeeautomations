import Swal from "sweetalert2";
import "sweetalert2/dist/sweetalert2.min.css";

export async function confirmAction(title: string, text: string, confirmButtonText: string) {
  const result = await Swal.fire({
    title,
    text,
    icon: "warning",
    showCancelButton: true,
    confirmButtonText,
    cancelButtonText: "Cancel",
    focusCancel: true,
  });
  return result.isConfirmed;
}

export function showLoading(title: string, text: string) {
  void Swal.fire({ title, text, allowOutsideClick: false, didOpen: () => Swal.showLoading() });
}

export function showSuccess(title: string, text: string) {
  return Swal.fire({ title, text, icon: "success", confirmButtonText: "Done" });
}

export function showError(error: unknown, fallback = "The request could not be completed.") {
  const candidate = error instanceof Error
    ? error.message
    : error && typeof error === "object" && "message" in error
      ? (error as { message?: unknown }).message
      : undefined;
  const message = typeof candidate === "string"
    ? candidate
    : candidate ? JSON.stringify(candidate) : fallback;
  return Swal.fire({ title: "Something went wrong", text: message, icon: "error" });
}

export function closeAlert() {
  Swal.close();
}
