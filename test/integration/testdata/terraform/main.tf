# Entities for the Terraform scenarios of both SDKs. Each suite picks its
# entity names with -tf-var prefix=...

variable "prefix" {
  type = string
}

locals {
  regions = toset(["eu", "us"])
}

resource "azurerm_servicebus_namespace" "main" {
  name                = "sb-${var.prefix}"
  location            = "East US"
  resource_group_name = "rg-${var.prefix}"
  sku                 = "Standard"
}

resource "azurerm_servicebus_queue" "inbox" {
  name               = "${var.prefix}-inbox"
  namespace_id       = azurerm_servicebus_namespace.main.id
  lock_duration      = "PT45S"
  max_delivery_count = 7
}

resource "azurerm_servicebus_topic" "events" {
  name         = "${var.prefix}-events"
  namespace_id = azurerm_servicebus_namespace.main.id
}

resource "azurerm_servicebus_subscription" "region" {
  for_each           = local.regions
  name               = each.key
  topic_id           = azurerm_servicebus_topic.events.id
  max_delivery_count = 3
}

resource "azurerm_servicebus_subscription_rule" "region" {
  for_each        = local.regions
  name            = "only-${each.key}"
  subscription_id = azurerm_servicebus_subscription.region[each.key].id
  filter_type     = "SqlFilter"
  sql_filter      = "region = '${each.key}'"
  action          = "SET routed = TRUE"
}

resource "azurerm_servicebus_subscription" "big" {
  name               = "big"
  topic_id           = azurerm_servicebus_topic.events.id
  max_delivery_count = 1
  forward_to         = azurerm_servicebus_queue.inbox.name
}

resource "azurerm_servicebus_subscription_rule" "big" {
  name            = "big"
  subscription_id = azurerm_servicebus_subscription.big.id
  filter_type     = "CorrelationFilter"

  correlation_filter {
    label = "big"
    properties = {
      size = "L"
    }
  }
}
