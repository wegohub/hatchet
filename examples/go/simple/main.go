package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/hatchet-dev/hatchet/pkg/cmdutils"
	hatchet "github.com/hatchet-dev/hatchet/sdks/go"
	hatchetotel "github.com/hatchet-dev/hatchet/sdks/go/opentelemetry"
	"go.opentelemetry.io/otel"
)

func main() {
	client, err := hatchet.NewClient()
	if err != nil {
		log.Fatalf("failed to create hatchet client: %v", err)
	}

	instrumentor, err := hatchetotel.NewInstrumentor()
	if err != nil {
		log.Fatalf("failed to create instrumentor: %v", err)
	}

	tracer := otel.Tracer("otel-simple-example")

	// > Declaring a Task
	type SimpleInput struct {
		Message string `json:"message"`
	}

	type SimpleOutput struct {
		Result string `json:"result"`
	}

	task := client.NewStandaloneTask("process-message", func(ctx hatchet.Context, input SimpleInput) (SimpleOutput, error) {
		// log example
		ctx.Log("simple example start")

		// trace example
		payCtx, paySpan := tracer.Start(ctx, "payment.process")

		_, tokenSpan := tracer.Start(payCtx, "payment.tokenize-card")
		time.Sleep(200 * time.Millisecond)
		tokenSpan.End()

		_, chargeSpan := tracer.Start(payCtx, "payment.charge")
		time.Sleep(400 * time.Millisecond)
		chargeSpan.End()

		paySpan.End()

		ctx.Log("simple example end")

		return SimpleOutput{
			Result: "Processed: " + input.Message,
		}, nil
	})

	_ = func() error {
		// > Running a Task
		result, err := task.Run(context.Background(), SimpleInput{Message: "Hello, World!"})
		if err != nil {
			return err
		}

		var resultOutput SimpleOutput
		err = result.Into(&result)
		if err != nil {
			return err
		}

		fmt.Println(resultOutput.Result)

		return nil
	}

	_ = func() error {
		// > Running a task without waiting
		runRef, err := task.RunNoWait(context.Background(), SimpleInput{Message: "Hello, World!"})
		if err != nil {
			return err
		}

		fmt.Println(runRef.RunId)

		// > Subscribing to results
		result, err := runRef.Result()
		if err != nil {
			return err
		}

		var resultOutput SimpleOutput
		err = result.TaskOutput("process-message").Into(&resultOutput)
		if err != nil {
			return err
		}

		fmt.Println(resultOutput.Result)

		workflow := client.NewWorkflow("parent-workflow")

		// > Spawning tasks from within a task
		parent := workflow.NewTask("parent-task", func(ctx hatchet.Context, input SimpleInput) (*SimpleOutput, error) {
			// Run the child task
			_, err := task.Run(ctx, SimpleInput{Message: input.Message})
			if err != nil {
				return nil, err
			}

			return &SimpleOutput{
				Result: "Processed: " + input.Message,
			}, nil
		})

		_ = parent

		// > Running Multiple Tasks
		var results []string
		var resultsMutex sync.Mutex
		var errs []error
		var errsMutex sync.Mutex

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			defer wg.Done()
			result, err := task.Run(context.Background(), SimpleInput{
				Message: "Hello, World!",
			})

			if err != nil {
				errsMutex.Lock()
				errs = append(errs, err)
				errsMutex.Unlock()
				return
			}

			resultsMutex.Lock()

			var resultOutput SimpleOutput
			err = result.Into(&resultOutput)
			if err != nil {
				return
			}
			results = append(results, resultOutput.Result)
			resultsMutex.Unlock()
		}()

		go func() {
			defer wg.Done()
			result, err := task.Run(context.Background(), SimpleInput{
				Message: "Hello, Moon!",
			})

			if err != nil {
				errsMutex.Lock()
				errs = append(errs, err)
				errsMutex.Unlock()
				return
			}

			resultsMutex.Lock()

			var resultOutput SimpleOutput
			err = result.Into(&resultOutput)
			if err != nil {
				return
			}
			results = append(results, resultOutput.Result)
			resultsMutex.Unlock()
		}()

		wg.Wait()

		return nil
	}

	// > Starting a worker
	worker, err := client.NewWorker("simple-worker", hatchet.WithWorkflows(task))
	if err != nil {
		log.Fatalf("failed to create worker: %v", err)
	}

	worker.Use(instrumentor.Middleware())

	interruptCtx, cancel := cmdutils.NewInterruptContext()
	defer cancel()

	go func() {
		<-interruptCtx.Done()
		if shutdownErr := instrumentor.Shutdown(context.Background()); shutdownErr != nil {
			log.Printf("failed to shutdown instrumentor: %v", shutdownErr)
		}
	}()

	err = worker.StartBlocking(interruptCtx)
	if err != nil {
		log.Fatalf("failed to start worker: %v", err)
	}
}
