import { appTitle, sourceBuildDescription } from "../src/app-definition";

export default function Page() {
  return (
    <main>
      <h1>{appTitle}</h1>
      <p>{sourceBuildDescription}</p>
    </main>
  );
}
