# Entities of the Terraform end-to-end scenario, shaped like production:
# one topic with a subscription per channel, created with for_each, each
# with a SQL rule. The test passes the channels with -tf-var.

variable "channels" {
  type = list(string)
}

variable "lock_duration" {
  type    = string
  default = "PT30S"
}

resource "azurerm_servicebus_namespace" "main" {
  name                = "sb-e2e"
  location            = "East US"
  resource_group_name = "rg-e2e"
  sku                 = "Standard"
}

resource "azurerm_servicebus_topic" "alerts" {
  name         = "alerts"
  namespace_id = azurerm_servicebus_namespace.main.id
}

resource "azurerm_servicebus_subscription" "channel" {
  for_each           = toset(var.channels)
  name               = each.key
  topic_id           = azurerm_servicebus_topic.alerts.id
  lock_duration      = var.lock_duration
  max_delivery_count = 3
}

resource "azurerm_servicebus_subscription_rule" "channel" {
  for_each        = azurerm_servicebus_subscription.channel
  name            = "only-${each.key}"
  subscription_id = each.value.id
  filter_type     = "SqlFilter"
  sql_filter      = "channel = '${each.key}'"
}

resource "azurerm_servicebus_subscription" "everything" {
  name               = "everything"
  topic_id           = azurerm_servicebus_topic.alerts.id
  max_delivery_count = 3
}
