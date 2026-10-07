# Dataclass Support

Throughout the docs, we use Pydantic models in virtually all of our Python examples for validating task inputs and outputs. This is the recommended path, as it provides lots of safety guarantees as you're writing tasks. With that said, Hatchet also supports using `dataclasses` as both input and output types to tasks. **Dataclass support was added in SDK version 1.21.0.**

> **Warning:** Dataclasses do not perform any type validation on instantiation like Pydantic
>   models do.

### Usage

To use a dataclass instead of a Pydantic model, you'll need to:

1. Provide an `input_validator` as a parameter to your `workflow` or `task` (in the case of a standalone task with `hatchet.task`).
2. Add return type hints for your `tasks`.

### Example Usage

`dataclass` validators work exactly like Pydantic models in Hatchet. First, you create the classes:

```python
@dataclass
class Input:
    name: str


@dataclass
class Output:
    message: str
```

And then you provide the classes to your workflow or task:

```python
@hatchet.task(input_validator=Input)
def say_hello(input: Input, ctx: Context) -> Output:
    return Output(message=f"Hello, {input.name}!")
```

And finally, triggering works the same as well - you just provide the dataclass instance as input:

```python
say_hello.run(input=Input(name="Hatchet"))
```
