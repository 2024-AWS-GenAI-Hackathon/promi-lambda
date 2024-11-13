import base64
import io
import json
import logging
import boto3
from PIL import Image, ImageDraw, ImageFont
import pymysql
from io import BytesIO
import os

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS 클라이언트 설정 및 환경 변수
S3_BUCKET = os.getenv("S3_BUCKET")
S3_FOLDER = os.getenv("S3_FOLDER", "Food/")
TARGET_BUCKET = os.getenv("TARGET_BUCKET")
TEXT_BUCKET = os.getenv("TEXT_BUCKET")
FONT_BUCKET = os.getenv("FONT_BUCKET")
FONT_KEY = os.getenv("FONT_KEY", "BagelFatOne-Regular.ttf")

TRANSLATE_CLIENT = boto3.client('translate')
S3_CLIENT = boto3.client('s3')
BEDROCK_CLIENT = boto3.client('bedrock-runtime', region_name="us-west-2")
response_list = []

def append_text_to_image(user_id, image_data, i):
    try:
        # 텍스트 파일 가져오기
        text_key = f"{user_id}/final_text.json"
        text_response = S3_CLIENT.get_object(Bucket=TEXT_BUCKET, Key=text_key)
        json_data = json.loads(text_response['Body'].read().decode('utf-8'))
        my_text = json_data.get('final_title', " ")

        # 이미지와 폰트 처리
        image = Image.open(BytesIO(image_data))
        draw = ImageDraw.Draw(image)

        # S3에서 폰트 파일 가져오기 및 로드
        font_data = S3_CLIENT.get_object(Bucket=FONT_BUCKET, Key=FONT_KEY)['Body'].read()
        font = ImageFont.truetype(BytesIO(font_data), 60)

        # 텍스트 위치 설정
        text_width, text_height = draw.textsize(my_text, font=font)
        image_width, image_height = image.size
        text_position = ((image_width - text_width) // 2, image_height - text_height - 20)
        draw.text(text_position, my_text, font=font, fill=(0, 0, 0))

        # 이미지를 BytesIO 객체로 변환 후 업로드
        img_byte_arr = BytesIO()
        image.save(img_byte_arr, format='JPEG')
        img_byte_arr.seek(0)

        output_image_key = f"{user_id}/pro_image_{i}.jpg"
        S3_CLIENT.put_object(Bucket=TARGET_BUCKET, Key=output_image_key, Body=img_byte_arr.getvalue(), ContentType='image/jpeg')

        s3_image_url = f"https://{TARGET_BUCKET}.s3.amazonaws.com/{output_image_key}"
        return s3_image_url

    except Exception as e:
        logger.error(f"이미지에 텍스트를 추가하는 중 오류 발생: {e}")
        raise

def get_latest_image_key(bucket, folder):
    try:
        response = S3_CLIENT.list_objects_v2(Bucket=bucket, Prefix=folder)
        jpg_files = [obj for obj in response.get('Contents', []) if obj['Key'].endswith('.jpg')]
        if not jpg_files:
            raise FileNotFoundError("지정된 폴더에 JPG 파일이 없습니다.")
        return max(jpg_files, key=lambda x: x['LastModified'])['Key']
    except Exception as e:
        logger.error(f"최신 이미지 키 가져오는 중 오류 발생: {e}")
        raise

def get_image_from_s3(bucket_name, object_key):
    try:
        response = S3_CLIENT.get_object(Bucket=bucket_name, Key=object_key)
        image_data = response['Body'].read()

        # 이미지 크기 조정
        image = Image.open(io.BytesIO(image_data))
        image.thumbnail((1024, 1024), Image.LANCZOS)

        buffered = io.BytesIO()
        image.save(buffered, format="JPEG")
        return base64.b64encode(buffered.getvalue()).decode('utf-8')
    except Exception as e:
        logger.error(f"S3에서 이미지 가져오는 중 오류 발생: {e}")
        raise

def get_rds_data(category, limit):
    try:
        connection = pymysql.connect(
            host=os.getenv("RDS_HOST"),
            user=os.getenv("RDS_USER"),
            password=os.getenv("RDS_PASSWORD"),
            database=os.getenv("RDS_DB"),
            cursorclass=pymysql.cursors.DictCursor
        )
        with connection.cursor() as cursor:
            cursor.execute("SELECT content FROM review WHERE category = %s LIMIT %s", (category, limit))
            return [row['content'] for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"RDS에서 데이터 조회 중 오류 발생: {e}")
        raise
    finally:
        if connection:
            connection.close()

def translate_text(text_list):
    try:
        return [
            TRANSLATE_CLIENT.translate_text(Text=text, SourceLanguageCode='ko', TargetLanguageCode='en')['TranslatedText']
            if text.strip() else "텍스트가 없습니다"
            for text in text_list
        ]
    except Exception as e:
        logger.error(f"텍스트 번역 중 오류 발생: {e}")
        raise

def translate_single_text(text):
    try:
        return TRANSLATE_CLIENT.translate_text(Text=text, SourceLanguageCode='ko', TargetLanguageCode='en')['TranslatedText'] if text.strip() else "텍스트가 없습니다"
    except Exception as e:
        logger.error(f"단일 텍스트 번역 중 오류 발생: {e}")
        raise

def generate_combined_text(prompt_text):
    try:
        prompt_text_english = translate_single_text(prompt_text)
        rds_data_vibe = get_rds_data("vibe", 25)
        rds_data_consumer = get_rds_data("consumer", 25)

        review_text = " ".join(translate_text(rds_data_vibe) + translate_text(rds_data_consumer))[:200]
        combined_text = f"Generate a new image based on the review text: '{review_text}' and the prompt: '{prompt_text_english}'."
        return combined_text[:512] if len(combined_text) > 512 else combined_text
    except Exception as e:
        logger.error(f"결합된 텍스트 생성 중 오류 발생: {e}")
        raise

def generate_image(model_id, body):
    try:
        response = BEDROCK_CLIENT.invoke_model(
            modelId=model_id,
            body=body,
            accept="application/json",
            contentType="application/json"
        )
        response_body = json.loads(response.get("body").read())
        
        base64_image = response_body.get("artifacts")[0].get("base64")
        return base64.b64decode(base64_image)

    except Exception as e:
        logger.error(f"이미지 생성 중 오류 발생: {e}")
        raise


def lambda_handler(event, context):
    try:
        body = json.loads(event['body'])
        prompt_text = body.get('prompt_text', "")
        action = body.get('action', "")
        user_id = body.get('user_id')

        for attempt in range(3):
            try:
                if 'body' not in event:
                    return {'statusCode': 400, 'body': json.dumps({"error": "이벤트에 본문이 없습니다"})}

                s3_key = get_latest_image_key(S3_BUCKET, S3_FOLDER)
                input_image = get_image_from_s3(S3_BUCKET, s3_key)

                if action == "create":
                    combined_text = generate_combined_text(prompt_text)
                    model_id = 'stability.stable-diffusion-xl-v1'
                    body = json.dumps({
                        "text_prompts": [{"text": combined_text}],
                        "init_image": input_image,
                        "style_preset": "isometric"
                    })
                    image_bytes = generate_image(model_id, body)
                    generated_image_base64 = base64.b64encode(image_bytes).decode('utf-8')

                    target_key = f"{user_id}_generated_image_attempt_{attempt + 1}.jpg"
                    image_data = base64.b64decode(generated_image_base64)
                    url = append_text_to_image(user_id, image_data, attempt+1)
                    response_list.append(url)

                elif action == "confirm":
                    generated_image_base64 = body.get("generated_image")
                    if not generated_image_base64:
                        return {'statusCode': 400, 'body': json.dumps({"error": "확인할 생성된 이미지가 없습니다"})}

                    image_data = base64.b64decode(generated_image_base64)
                    response_list.append(append_text_to_image(user_id, image_data, attempt+1))

            except Exception as e:
                logger.error(f"시도 {attempt + 1}에서 오류 발생: {e}")
                if attempt == 2:
                    return {'statusCode': 500, 'body': json.dumps({"error": str(e)}), 'headers': {'Content-Type': 'application/json'}}

        if action == "create":
            return {
                'statusCode': 200,
                'body': json.dumps({"generated_image": response_list, "message": f"S3에 이미지가 성공적으로 생성 및 저장되었습니다: {target_key}"}),
                'headers': {'Content-Type': 'application/json'}
            }
        elif action == "confirm":
            return {'statusCode': 200, 'body': json.dumps({"message": "이미지가 성공적으로 저장되었습니다"}), 'headers': {'Content-Type': 'application/json'}}

    except Exception as e:
        logger.error(f"lambda_handler에서 예기치 않은 오류 발생: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({"error": "서버 오류가 발생했습니다"}),
            'headers': {'Content-Type': 'application/json'}
        }
