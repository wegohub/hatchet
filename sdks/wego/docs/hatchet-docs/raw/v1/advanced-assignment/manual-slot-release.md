# Manual Slot Release

The Hatchet execution model sets a number of available slots for running tasks in a workflow. When a task is running, it occupies a slot, and if a worker has no available slots, it will not be able to run any more tasks concurrently.

In some cases, you may have a task in your workflow that is resource-intensive and requires exclusive access to a shared resource, such as a database connection or a GPU compute instance. To ensure that other tasks in the workflow can run concurrently, you can manually release the slot after the resource-intensive task has completed, but the task still has non-resource-intensive work to do (i.e. upload or cleanup).

> **Warning:** This is an advanced feature and should be used with caution. Manually
>   releasing the slot can have unintended side effects on system performance and
>   concurrency. For example, if the worker running the task dies, the task will
>   not be reassigned and will remain in a running state until manually
>   terminated.

## Using Manual Slot Release

You can manually release a slot in from within a running task in your workflow using the Hatchet context method `release_slot`:

#### Go

```python
slot_release_workflow = hatchet.workflow(name="SlotReleaseWorkflow")


@slot_release_workflow.task()
def step1(input: EmptyModel, ctx: Context) -> dict[str, str]:
    print("RESOURCE INTENSIVE PROCESS")
    time.sleep(10)

    # 👀 Release the slot after the resource-intensive process, so that other steps can run
    ctx.release_slot()

    print("NON RESOURCE INTENSIVE PROCESS")
    return {"status": "success"}
```

#### Ruby

```go
_ = workflow.NewTask("step1", func(ctx hatchet.Context, _ any) (*StepOutput, error) {
	fmt.Println("RESOURCE INTENSIVE PROCESS")
	time.Sleep(10 * time.Second)

	// Release the slot after the resource-intensive process,
	// so that other steps can run on this worker.
	if releaseErr := ctx.ReleaseSlot(); releaseErr != nil {
		return nil, fmt.Errorf("failed to release slot: %w", releaseErr)
	}

	fmt.Println("NON RESOURCE INTENSIVE PROCESS")

	return &StepOutput{Status: "success"}, nil
})
```

#### Tab 3

```ruby
SLOT_RELEASE_WORKFLOW = HATCHET.workflow(name: "SlotReleaseWorkflow")

SLOT_RELEASE_WORKFLOW.task(:step1) do |input, ctx|
  puts "RESOURCE INTENSIVE PROCESS"
  sleep 10

  # Release the slot after the resource-intensive process, so that other steps can run
  ctx.release_slot

  puts "NON RESOURCE INTENSIVE PROCESS"
  { "status" => "success" }
end
```

In the above examples, the `release_slot()` method is called after the resource-intensive process has completed. This allows other tasks in the workflow to start executing while the current task continues with non-resource-intensive tasks.

> **Info:** Manually releasing the slot does not terminate the current task. The task will
>   continue executing until it completes or encounters an error.

## Use Cases

Some common use cases for Manual Slot Release include:

- Performing data processing or analysis that requires significant CPU, GPU, or memory resources
- Acquiring locks or semaphores to access shared resources
- Executing long-running tasks that don't need to block other tasks after some initial work is done

By utilizing Manual Slot Release, you can optimize the concurrency and resource utilization of your workflows, allowing multiple tasks to run in parallel when possible.
