import { useEffect, useRef, useState } from "react";

/** Reports an element's own content width, re-measured whenever it changes, via a ref to attach to it. Width starts
 * at 0 and is only real after the first measurement lands — callers that would render something wrong at zero width
 * should wait for it rather than guess.
 *
 * Unlike ./useReportSize this keeps the value locally instead of handing it to a parent: the one thing that needs it
 * (see ../components/ConveyorItems, which has to know how many fixed-width children fit) is also the element being
 * measured, so there is nothing to report upwards. */
export function useElementWidth(): { ref: React.RefObject<HTMLDivElement | null>; width: number } {
  const ref = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (!entry) return;
      setWidth(Math.floor(entry.contentRect.width)); // floor, so a fractional width never over-promises room
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  return { ref, width };
}
