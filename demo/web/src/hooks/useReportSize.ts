import { useEffect, useRef } from "react";

/** Reports a node's rendered (width, height) to the caller whenever it changes, via a ref to attach to the node's
 * root element. Used to lay the pipeline out from measured sizes instead of a fixed grid — see PipelineCanvas. */
export function useReportSize(id: string, onSize: (id: string, w: number, h: number) => void) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (!entry) return;
      const { width, height } = entry.contentRect;
      onSize(id, Math.ceil(width), Math.ceil(height));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, [id, onSize]);

  return ref;
}
