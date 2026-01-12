import boto3
import json

# 初始化Textract客户端
textract = boto3.client('textract')

def lambda_handler(event, context):
    # 从事件中获取S3桶和文件
    bucket = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    
    # 调用Textract识别文字
    response = textract.detect_document_text(
        Document={
            'S3Object': {
                'Bucket': bucket,
                'Name': file_key
            }
        }
    )
    
    # 提取识别结果
    extracted_text = [item['Text'] for item in response['Blocks'] if item['BlockType'] == 'LINE']
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'file_key': file_key,
            'extracted_text': extracted_text
        })
    }