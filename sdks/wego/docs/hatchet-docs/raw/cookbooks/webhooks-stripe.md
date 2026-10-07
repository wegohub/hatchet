# Stripe Webhooks

Stripe sends webhooks for all sorts of events, such as payments succeeding, subscription cancellations, invoice creation, and so on. This guide walks through setting up webhooks from Stripe to trigger events directly in Hatchet.

## Setup


### Get your Stripe webhook signing secret

In the [Stripe Dashboard](https://dashboard.stripe.com/webhooks), you'll create a new webhook endpoint. Don't fill in the URL yet — you'll get that from Hatchet in the next step. See [Stripe's webhooks guide](https://docs.stripe.com/webhooks) for more details on setting this up.

For now, note the **signing secret** that Stripe generates for you (it starts with `whsec_`). You'll need this to tell Hatchet how to verify incoming requests.

### Create the webhook in Hatchet

In the Hatchet dashboard, go to **Webhooks** and create a new webhook with the following settings:

Field, Value

**Name**, `stripe` (or whatever you'd like)
**Source**, Stripe
**Event Key Expression**, `'stripe:' + input.type`
**Secret**, Your `whsec_...` signing secret

The event key expression here takes the `type` field from Stripe's payload (something like `payment_intent.created`) and prefixes it with `stripe:` so your event keys are namespaced. When a webhook from Stripe is ingested with an `input.type` of `payment_intent.created`, there will be a corresponding Hatchet event with a key of `stripe:payment_intent.created` created.

Once you've created the webhook, copy the URL that Hatchet generates.

### Add the URL to Stripe

Go back to the Stripe Dashboard webhook you created in step 1 and paste in the Hatchet webhook URL. Select the events you want to listen for (or just select all of them — Hatchet will only trigger workflows that match the event key).

### Write a task that listens for Stripe events

Now you just need a task with a matching `on_events` trigger. For example, to handle successful payments:

#### Python

```python
class StripeObject(BaseModel):
    customer: str
    amount: int


class StripeData(BaseModel):
    object: StripeObject


class StripePaymentInput(BaseModel):
    type: str
    data: StripeData


class StripePaymentOutput(BaseModel):
    customer: str
    amount: int


@hatchet.task(
    input_validator=StripePaymentInput,
    on_events=["stripe:payment_intent.succeeded"],
)
def handle_stripe_payment(
    input: StripePaymentInput, ctx: Context
) -> StripePaymentOutput:
    customer = input.data.object.customer
    amount = input.data.object.amount
    print(f"Payment of {amount} from {customer}")
    return StripePaymentOutput(customer=customer, amount=amount)
```

#### Typescript

```typescript
type StripePaymentInput = {
  type: string;
  data: {
    object: {
      customer: string;
      amount: number;
    };
  };
};

export const handleStripePayment = hatchet.task({
  name: 'handle-stripe-payment',
  on: {
    event: 'stripe:payment_intent.succeeded',
  },
  fn: async (input: StripePaymentInput, ctx) => {
    const { customer, amount } = input.data.object;
    ctx.logger.info(`Payment of ${amount} from ${customer}`);
    return { customer, amount };
  },
});
```

#### Go

```go
type StripePaymentInput struct {
	Type string `json:"type"`
	Data struct {
		Object struct {
			Customer string `json:"customer"`
			Amount   int    `json:"amount"`
		} `json:"object"`
	} `json:"data"`
}

stripePayment := client.NewStandaloneTask(
	"handle-stripe-payment",
	func(ctx hatchet.Context, input StripePaymentInput) (*struct {
		Customer string `json:"customer"`
		Amount   int    `json:"amount"`
	}, error) {
		fmt.Printf("Payment of %d from %s\n", input.Data.Object.Amount, input.Data.Object.Customer)
		return &struct {
			Customer string `json:"customer"`
			Amount   int    `json:"amount"`
		}{
			Customer: input.Data.Object.Customer,
			Amount:   input.Data.Object.Amount,
		}, nil
	},
	hatchet.WithWorkflowEvents("stripe:payment_intent.succeeded"),
)
```

#### Ruby

```ruby
HANDLE_STRIPE_PAYMENT = HATCHET.task(
  name: "handle-stripe-payment",
  on_events: ["stripe:payment_intent.succeeded"]
) do |input, ctx|
  customer = input["data"]["object"]["customer"]
  amount = input["data"]["object"]["amount"]
  puts "Payment of #{amount} from #{customer}"
  { "customer" => customer, "amount" => amount }
end
```

### Test it

You can use Stripe's "Send test webhook" feature in the dashboard, or trigger a real event in test mode. You should see the task run appear in the Hatchet dashboard.
