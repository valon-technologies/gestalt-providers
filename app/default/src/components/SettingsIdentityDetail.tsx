import { useParams } from "@tanstack/react-router";
import ManagedIdentityDetailView from "@/components/ManagedIdentityDetailView";
import { subjectIdFromLocalParam } from "@/lib/managed-identity-paths";

export default function SettingsIdentityDetail() {
  const { identityLocalId } = useParams({ strict: false });
  const identityID = subjectIdFromLocalParam(identityLocalId ?? "");

  return (
    <ManagedIdentityDetailView
      identityID={identityID}
      embedded
      listTo="/settings/identities"
    />
  );
}
