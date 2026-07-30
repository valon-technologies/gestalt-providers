import { Alert, AlertActions, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";

export default function ErrorNotice({
  message,
  onRetry,
  retrying = false,
  className,
}: {
  message: string;
  onRetry?: () => void;
  retrying?: boolean;
  className?: string;
}) {
  return (
    <Alert variant="destructive" className={className} data-testid="error-notice">
      <AlertDescription>{message}</AlertDescription>
      {onRetry ? (
        <AlertActions>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={onRetry}
            disabled={retrying}
          >
            {retrying ? "Retrying…" : "Retry"}
          </Button>
        </AlertActions>
      ) : null}
    </Alert>
  );
}
