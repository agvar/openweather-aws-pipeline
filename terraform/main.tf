resource "aws_cloudwatch_event_rule" "weather_collection_schedule" {
  name                = "weather-collection-schedule"
  description         = "Trigger weather collection twice daily"
  schedule_expression = "cron(0 6,18 * * ? *)"
}

