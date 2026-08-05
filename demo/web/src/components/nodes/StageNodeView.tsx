import { memo } from "react";
import type { NodeProps } from "@xyflow/react";
import { Handle, Position } from "@xyflow/react";
import { useReportSize } from "../../hooks/useReportSize";
import { StageBox } from "./StageBox";
import type { StageNodeData } from "./types";

// memo matters here specifically because PipelineCanvas hands back a referentially-stable `data` prop for a node
// whose resolved state hasn't changed since the last poll tick (see resolvePipeline and PipelineCanvas's own node
// cache) — without memo, React would still re-run this component's render on every tick regardless.
export const StageNodeView = memo(function StageNodeView(props: NodeProps) {
  const d = props.data as unknown as StageNodeData;
  const ref = useReportSize(d.stage.id, d.reportSize);

  return (
    <StageBox
      stage={d.stage}
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
