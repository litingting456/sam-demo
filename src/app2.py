import boto3
import json
from datetime import datetime

# 初始化AWS服务客户端
s3 = boto3.client('s3')
rekognition = boto3.client('rekognition')
dynamodb = boto3.resource('dynamodb')

# 指定DynamoDB表名（需提前创建，或在SAM模板中定义）
TABLE_NAME = 'ImageRecognitionResults'
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    """
    S3上传图片触发的Lambda处理函数
    :param event: S3触发事件（包含桶名、文件键等信息）
    :param context: Lambda运行上下文
    :return: 识别结果与存储状态
    """
    try:
        # 1. 从S3事件中提取核心信息
        s3_event = event['Records'][0]['s3']
        bucket_name = s3_event['bucket']['name']
        file_key = s3_event['object']['key']
        file_size = s3_event['object'].get('size', 0)  # 图片大小（字节）
        
        # 2. 调用Rekognition识别图片标签
        rek_response = rekognition.detect_labels(
            Image={
                'S3Object': {
                    'Bucket': bucket_name,
                    'Name': file_key
                }
            },
            MaxLabels=10,  # 最多识别10个标签
            MinConfidence=70  # 置信度阈值70%
        )
        
        # 3. 整理识别结果
        labels = []
        for label in rek_response['Labels']:
            labels.append({
                'Name': label['Name'],
                'Confidence': round(label['Confidence'], 2),
                'Instances': len(label.get('Instances', []))
            })
        
        # 4. 将结果存入DynamoDB
        item = {
            'ImageId': f"{bucket_name}_{file_key}",  # 主键（确保唯一）
            'BucketName': bucket_name,
            'FileKey': file_key,
            'FileSize': file_size,
            'UploadTime': datetime.now().isoformat(),
            'Labels': labels,
            'LabelCount': len(labels)
        }
        
        table.put_item(Item=item)
        
        # 5. 返回成功响应
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'message': '图片识别完成，结果已存入DynamoDB',
                'image_info': {'bucket': bucket_name, 'file_key': file_key},
                'label_count': len(labels),
                'labels': labels
            })
        }
    
    except Exception as e:
        # 异常处理（打印日志+返回错误）
        print(f"处理失败：{str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'message': '处理失败',
                'error': str(e)
            })
        }