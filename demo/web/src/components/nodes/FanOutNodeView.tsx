import { memo } from "react";
import type { NodeProps } from "@xyflow/react";
import { Handle, Position } from "@xyflow/react";
import { useReportSize } from "../../hooks/useReportSize";
import { FanOutBox } from "./FanOutBox";
import type { FanOutNodeData } from "./types";

// Wrapped in memo for the same reason as StageNodeView — see its own comment.
export const FanOutNodeView = memo(function FanOutNodeView(props: NodeProps) {
  const d = props.data as unknown as FanOutNodeData;
  const ref = useReportSize(d.fanout.id, d.reportSize);

  return (
    <FanOutBox
      fanout={d.fanout}
      callbacks={d.callbacks}
      shellRef={ref}
      handles={
        <>
          <Handle type="target" position={Position.Left} isConnectable={false} />
          <Handle type="source" position={Position.Right} isConnectable={false} />
        </>
      }
    />
  );
});
