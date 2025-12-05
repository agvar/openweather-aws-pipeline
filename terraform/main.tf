provider "aws"{
  region = "us-east-1"
}

variable "create_ephemeral_resources_flag"{
  description = "Create ephemeral resources"
  type = bool
  default = true
}

data "aws_iam_role" "github_actions_aws"{
  name="github-actions-weather-app"
}

#################################
# List of Persistent resources
#################################

resource "aws_s3_bucket" "weatherDataCode"{
  bucket = "weatherdatacode-bucket-11-07-2025-12-20-AM-avar"
}

resource "aws_s3_bucket" "weatherDataStore"{
  bucket = "weatherdatastore-bucket-11-07-2025-12-20-AM-avar"
}

resource "aws_iam_role" "lambda_execution_role"{
  name = "weather-lambda-execution-role"
  assume_role_policy =  jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

}

#################################
# List of Ephemeral Resources
#################################

resource "aws_lambda_function" "weather_collector_lambda"{
  count = var.create_ephemeral_resources_flag ? 1 : 0
  function_name="weather-collector-lambda"
  role = aws_iam_role.lambda_execution_role.arn
  handler = "WeatherCollector_lambda_handler.lambda_handler"
  runtime = "python3.9"
  timeout = 300
  s3_bucket = aws_s3_bucket.weatherDataCode.bucket
  s3_key = "weather-collector.zip"

}

resource "aws_cloudwatch_event_rule" "weather_collection_schedule" {
  count = var.create_ephemeral_resources_flag ? 1 :0 
  name  = "weather-collection-schedule"
  description         = "Trigger weather collection twice daily"
  schedule_expression = "cron(0 6,18 * * ? *)"
}

resource "aws_cloudwatch_event_target" "trigger_lambda"{
  count = var.create_ephemeral_resources_flag ? 1 :0 
  rule = aws_cloudwatch_event_rule.weather_collection_schedule[0].name
  arn = aws_lambda_function.weather_collector_lambda[0].arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  count         = var.create_ephemeral_resources_flag ? 1 : 0
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.weather_collector_lambda[0].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.weather_collection_schedule[0].arn
}

resource "aws_cloudwatch_log_group" "weather_collector_lambda_logs"{
  count = var.create_ephemeral_resources_flag ? 1 :0 
  name = "/aws/lambda/weather-collector-lambda-logs"
  retention_in_days = 14
}

resource "aws_iam_role_policy" "lambda_execution_policy"{
    role = aws_iam_role.lambda_execution_role.id
    policy = jsonencode({
    Version = "2012-10-17"
    Statement = [

    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream", 
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    },  

      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.weatherDataStore.arn,
          "${aws_s3_bucket.weatherDataStore.arn}/*",
          aws_s3_bucket.weatherDataCode.arn,
          "${aws_s3_bucket.weatherDataCode.arn}/*"
        ]
      },
      {
        "Effect": "Allow",
        "Action": [
          "ssm:GetParameter"
        ],
        "Resource": "arn:aws:ssm:us-east-1:049506468119:parameter/Openweatherapi_key"
      }
    ]
    })
    }

resource "aws_iam_role_policy" "github_actions"{
    role = data.aws_iam_role.github_actions_aws.id
    policy = jsonencode({
    Version = "2012-10-17"
    Statement = [

      {
        Effect = "Allow"
        Action = [
          "lambda:UpdateFunctionCode",      # Update Lambda code
          "lambda:GetFunction",             # Check Lambda exists
          "lambda:InvokeFunction"           # Test Lambda after deploy
        ]
        Resource = aws_lambda_function.weather_collector_lambda[0].arn
      },    
        {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.weatherDataStore.arn,
          "${aws_s3_bucket.weatherDataStore.arn}/*",
          aws_s3_bucket.weatherDataCode.arn,
          "${aws_s3_bucket.weatherDataCode.arn}/*"
        ]
      },
              {
        Effect = "Allow"
        Action = [
          "iam:CreateRole",
          "iam:GetRole",
          "iam:DeleteRole",
          "iam:PutRolePolicy",
          "iam:GetRolePolicy",
          "iam:DeleteRolePolicy",
          "iam:AttachRolePolicy",
          "iam:DetachRolePolicy",
          "iam:ListAttachedRolePolicies",
          "iam:ListRolePolicies",
          "iam:PassRole"
        ]
        Resource = [
          aws_iam_role.lambda_execution_role.arn,
          data.aws_iam_role.github_actions_aws.id:policy/*
        ]
      }
      ]
  })
  }