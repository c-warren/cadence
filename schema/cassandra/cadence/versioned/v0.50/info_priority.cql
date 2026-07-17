-- Add priority to timer_info and child_execution_info UDTs so that the
-- task priority stored on TimerInfo/ChildExecutionInfo survives a shard reload
-- and task regeneration produces correctly-prioritized tasks.
ALTER TYPE timer_info ADD priority int;
ALTER TYPE child_execution_info ADD priority int;
