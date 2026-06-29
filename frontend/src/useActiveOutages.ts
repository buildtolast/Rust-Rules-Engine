import { useState, useEffect } from "react";
import type { Incident } from "./types";

function isOneShot(name: string): boolean {
  const bare = name.replace(/^rre-/, "");
  return (
    bare.endsWith("-init") ||
    bare.endsWith("-patch") ||
    bare.endsWith("-migration") ||
    bare.includes("init")
  );
}

export function useActiveOutages(): Incident[] {
  const [active, setActive] = useState<Incident[]>([]);

  useEffect(() => {
    const fetch_ = () =>
      fetch("/api/sre/outages")
        .then((r) => (r.ok ? r.json() : []))
        .then((data: Incident[]) => {
          const next = data.filter((i) => i.active && !isOneShot(i.container));
          setActive((prev) => {
            const prevKeys = prev
              .map((i) => i.container)
              .sort()
              .join(",");
            const nextKeys = next
              .map((i) => i.container)
              .sort()
              .join(",");
            return prevKeys === nextKeys ? prev : next;
          });
        })
        .catch(() => {});

    fetch_();
    const t = setInterval(fetch_, 30_000);
    return () => clearInterval(t);
  }, []);

  return active;
}
