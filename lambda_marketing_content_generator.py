import json
import pymysql
import boto3
import base64
import os
from datetime import datetime

# RDS와 S3 연결 정보
RDS_HOST = os.getenv("RDS_HOST")
RDS_USER = os.getenv("RDS_USER")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")
RDS_DB = os.getenv("RDS_DB")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

MODEL_ID = "anthropic.claude-3-5-sonnet-20240620-v1:0"
bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")

s3_client = boto3.client('s3')

def connect_to_rds():
    return pymysql.connect(
        host=RDS_HOST,
        user=RDS_USER,
        password=RDS_PASSWORD,
        database=RDS_DB,
        cursorclass=pymysql.cursors.DictCursor
    )

def upload_image_to_s3(image_data, category):
    if not image_data:
        raise ValueError("Image data is required and cannot be None")

    image_bytes = base64.b64decode(image_data)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    image_key = f"{category}/image_{timestamp}.jpg"

    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=image_key,
        Body=image_bytes,
        ContentType='image/jpeg'
    )
    s3_url = f"https://{S3_BUCKET_NAME}.s3.amazonaws.com/{image_key}"

    return s3_url

# Bedrock 모델에 대한 응답을 가져오는 함수
def claude_model_get_response(reviews, additional_requests):
    prompt = f"""
        {reviews}는 우리 가게의 손님들이 남긴 후기입니다. 이 리뷰들을 바탕으로, {additional_requests}를 강조하거나 주제로 포함하여 인스타그램 마케팅용 제목과 본문을 각각 3개씩 작성해 주세요. 제목은 30자 이내, 본문은 200자 이내로 작성해 주시고, 한국어로 작성해 주세요. 

        답변 형식은 다음과 같습니다:

        "first_title" = "...",
        "first_content" = "...",
        "second_title" = "...",
        "second_content" = "...",
        "third_title" = "...",
        "third_content" = "...",

        """

    try:
        body = json.dumps(
            {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1000,
                "temperature": 0.99,
                "top_p": 0.99,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": prompt
                            }
                        ]
                    }
                ]
            }
        )

        response = bedrock_runtime.invoke_model(
            modelId=MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(body)
        )				
        response_body = json.loads(response.get("body").read())
        output_text = response_body["content"][0]["text"]
        return output_text
        
    except Exception as e:
        print(e)


def lambda_handler(event, context):
    body = json.loads(event['body'])
    category = body.get('category')
    image_data = body.get('image')
    posting_time = body.get('posting_time')
    additional_requests = body.get('additional_requests', None)
    connection = None

    try:
        # 1. 이미지 S3에 업로드
        s3_image_url = upload_image_to_s3(image_data, category)

        # 2. RDS에서 데이터 조회
        connection = connect_to_rds()
        with connection.cursor() as cursor:
            query = """
            SELECT * FROM review
            WHERE category = %s
            """
            cursor.execute(query, (category,))
            reviews = cursor.fetchall()
        
        # 3. 모델 응답 생성
        res = claude_model_get_response(reviews, additional_requests)
        response_json = json.dumps({
                        'reviews': reviews,
                        's3_image_url': s3_image_url,
                        'additional_requests': additional_requests,
                        'model_response': res
                    }, ensure_ascii=False)
        response_bytes = response_json.encode('utf-8')

        # 4. 응답 데이터 반환
        return {
            'statusCode': 200,
            'body': response_json,
            'headers': {
                'Content-Type': 'application/json'
            }
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
    finally:
        if connection:
            connection.close()
