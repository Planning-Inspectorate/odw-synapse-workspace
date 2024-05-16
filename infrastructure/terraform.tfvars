# common variables loaded by default
# see https://developer.hashicorp.com/terraform/language/values/variables#variable-definitions-tfvars-files

odt_appeals_back_office_sb_topic_subscriptions = [
  {
    subscription_name = "appeal-odw-sub"
    topic_name        = "appeal"
  },
  {
    subscription_name = "appeal-document-odw-sub"
    topic_name        = "appeal-document"
  },
  {
    subscription_name = "appeal-event-odw-sub"
    topic_name        = "appeal-event"
  },
  {
    subscription_name = "appeal-service-user-odw-sub"
    topic_name        = "appeal-service-user"
  }
]
