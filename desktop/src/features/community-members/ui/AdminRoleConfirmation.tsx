import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/shared/ui/alert-dialog";

/** Explicit confirmation gate for the exceptional community-admin role. */
export function AdminRoleConfirmation({
  count = 1,
  disabled,
  onConfirm,
  onOpenChange,
  open,
}: {
  count?: number;
  disabled: boolean;
  onConfirm: () => void;
  onOpenChange: (open: boolean) => void;
  open: boolean;
}) {
  return (
    <AlertDialog onOpenChange={onOpenChange} open={open}>
      <AlertDialogContent data-testid="grant-admin-confirmation">
        <AlertDialogHeader>
          <AlertDialogTitle>Grant community admin access?</AlertDialogTitle>
          <AlertDialogDescription>
            {count === 1 ? "This person" : `These ${count} people`} will be able
            to invite and remove members. Only a community owner can grant this
            role.
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel disabled={disabled}>Cancel</AlertDialogCancel>
          <AlertDialogAction disabled={disabled} onClick={onConfirm}>
            Grant admin access
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}
