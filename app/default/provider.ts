import { defineApp, ok, operation, response, s } from "@valon-technologies/gestalt";

export const provider = defineApp({
  displayName: "Home",
  description: "Default Gestalt UI bundle served at /.",
  connectionMode: "none",
  operations: [
    operation({
      id: "static",
      method: "GET",
      description: "Serve the home UI bundle.",
      input: s.object({}),
      async handler() {
        return ok(response(200, "", {}));
      },
    }),
  ],
});
