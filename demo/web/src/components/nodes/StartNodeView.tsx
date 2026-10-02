import { memo } from "react";
import type { NodeProps } from "@xyflow/react";
import { Handle, Position } from "@xyflow/react";
import { useReportSize } from "../../hooks/useReportSize";
import { StartBox } from "./StartBox";
import { START_ID } from "../../types/topology";
import type { StartNodeData } from "./types";

/** The implicit start stage: only "Add stage after" in its context menu — see the spec. Its own Delay slider is the
 * simulated time an item spends here before its first move, i.e. before entering the pipeline, and its title is
 * click-to-rename like any other node's ("Read" until then). Wrapped in memo for the same reason as StageNodeView —
 * see its own comment. */
export const StartNodeView = memo(function StartNodeView(props: NodeProps) {
  const d = props.data as unknown as StartNodeData;
  const ref = useReportSize(START_ID, d.reportSize);

  return (
    <StartBox
      nodeId={START_ID}
      label={d.name}
      onRename={d.onRename}
      delayMs={d.delayMs}
      onEditDelay={d.onEditDelay}
      shellRef={ref}
      onContextMenu={(e) => {
        e.preventDefault();
        d.onContextMenu({ kind: "start" }, e);
      }}
      handles={<Handle type="source" position={Position.Right} isConnectable={false} />}
    />
  );
});
