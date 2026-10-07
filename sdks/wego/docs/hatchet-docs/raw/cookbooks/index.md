# Cookbooks

Our cookbooks are guides to help you solve common problems you'll find easy to tackle with Hatchet.

## Webhooks

Receive webhooks from external services and use them to trigger tasks in Hatchet. Each guide walks through setup end-to-end — creating the webhook in Hatchet, wiring it up to the source, and writing a task that handles the incoming event.


  
    Handle payment events, subscription changes, and other Stripe webhooks.
  
  
    React to pushes, pull requests, issues, and other GitHub events.
  
  
    Respond to Slack events like messages, reactions, and slash commands.
  


## Workflow Patterns

Build practical end-to-end workflows with Hatchet’s durable execution, event handling, and task orchestration primitives.


  
    Build a durable support workflow that triages tickets, waits for customer
    replies, and escalates on timeout.
  
  
    Send a welcome email after signup, wait for onboarding completion, and send
    a follow-up only if the user does not finish in time.
  
  
    Build a DAG workflow that extracts text from a PDF, classifies it, and
    summarizes the content.
  
  
    Build a highly parallelized object processor for Amazon S3.
  


## Agent Patterns

Expose Hatchet workflows and tasks as tools for AI agents via MCP and agent SDKs.


  
    Expose Hatchet workflows and tasks as MCP-compatible agent tools with
    durable execution, retries, and observability behind every tool call.
  
  
    Use the Claude Agent SDK with Hatchet-backed MCP tools in a trusted
    environment.
  
  
    Use the OpenAI Agents SDK with Hatchet-backed tools in a trusted
    environment.
