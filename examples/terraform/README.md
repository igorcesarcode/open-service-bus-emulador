# Terraform

[`main.tf`](main.tf) is a Terraform file for Azure Service Bus with
`azurerm_servicebus_namespace`, queues, topics (some created with `for_each`),
subscriptions, rules, authorization rules, a role assignment and a resource
lock. The emulator reads it **without running Terraform** and creates the same
entities locally:

```bash
open-service-bus -terraform examples/terraform
open-service-bus -terraform examples/terraform/main.tf -tf-var environment=dev
```

Variables come from `-tf-var name=value`, `-tf-var-file`, `terraform.tfvars`,
`*.auto.tfvars` and their `.json` forms. Locals, `count`, `for_each`,
`dynamic` blocks, references and common functions are evaluated.

Point it at the Terraform of your real infrastructure to get the same queues,
topics and subscriptions locally. A module whose source is not available
(a private registry, say) is handled by an adapter that recognizes the
arguments `asb_queues`, `asb_topics_name` and `asb_subs`; use
`-tf-module-map` to name other arguments. See
[Configuration](../../docs/configuration.md#terraform).

The reverse also works: `open-service-bus -db osb.db -export ./out` writes the
current state as `main.tf`.
