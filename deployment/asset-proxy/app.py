#!/usr/bin/env python3
"""
S3 Asset Proxy Service for STAC
Streams S3 assets directly using IAM role credentials
"""
import os
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse, Response
from fastapi.middleware.cors import CORSMiddleware
import boto3
from botocore.exceptions import ClientError
import uvicorn

app = FastAPI(title="STAC Asset Proxy")

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['GET', 'HEAD'],
    allow_headers=['*'],
)

# S3 client with IAM role credentials
s3_client = boto3.client('s3', region_name=os.environ.get('AWS_REGION', 'us-east-1'))

# TODO: Sep 2026:  update this to match discoveries and updates in the user_data_standalone.sh.tpl file.


@app.get('/health')
@app.head('/health')
def health_check():
    """Health check endpoint"""
    return {'status': 'ok', 'service': 'asset-proxy'}


@app.head('/s3/{bucket}/{path:path}')
def head_s3_asset(bucket: str, path: str):
    """
    HEAD request for S3 asset metadata.

    Args:
        bucket: S3 bucket name
        path: Object key path

    Returns:
        Response with S3 object metadata headers
    """
    try:
        response = s3_client.head_object(
            Bucket=bucket,
            Key=path
        )

        headers = {
            'Content-Type': response.get('ContentType', 'application/octet-stream'),
            'Content-Length': str(response.get('ContentLength', 0)),
            'Last-Modified': response.get('LastModified', '').strftime('%a, %d %b %Y %H:%M:%S GMT') if response.get('LastModified') else '',
            'ETag': response.get('ETag', ''),
            'Accept-Ranges': 'bytes',
        }

        return Response(headers=headers, status_code=200)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            raise HTTPException(status_code=404, detail=f"Object not found: {bucket}/{path}")
        else:
            raise HTTPException(status_code=403, detail=f"Access denied: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get('/s3/{bucket}/{path:path}')
def proxy_s3_asset(bucket: str, path: str, request: Request):
    """
    Stream S3 asset content directly using IAM role credentials.

    Supports HTTP Range requests for COG/GeoTIFF rendering in browsers.

    Args:
        bucket: S3 bucket name
        path: Object key path
        request: FastAPI request object

    Returns:
        StreamingResponse with S3 object content (full or partial)
    """
    try:
        # First get object metadata to know the total size
        head_response = s3_client.head_object(
            Bucket=bucket,
            Key=path
        )
        total_size = head_response['ContentLength']

        # Parse Range header if present
        range_header = request.headers.get('range')

        if range_header and range_header.startswith('bytes='):
            # Parse range (e.g., "bytes=0-1023")
            range_spec = range_header.replace('bytes=', '')
            range_parts = range_spec.split('-')

            start = int(range_parts[0]) if range_parts[0] else 0
            end = int(range_parts[1]) if len(range_parts) > 1 and range_parts[1] else total_size - 1

            # Ensure end doesn't exceed file size
            end = min(end, total_size - 1)
            content_length = end - start + 1

            # Get object with range
            response = s3_client.get_object(
                Bucket=bucket,
                Key=path,
                Range=f'bytes={start}-{end}'
            )

            # Stream the partial content
            def generate():
                for chunk in response['Body'].iter_chunks(chunk_size=65536):
                    yield chunk

            headers = {
                'Content-Type': head_response.get('ContentType', 'application/octet-stream'),
                'Content-Length': str(content_length),
                'Content-Range': f'bytes {start}-{end}/{total_size}',
                'Accept-Ranges': 'bytes',
                'Last-Modified': head_response.get('LastModified', '').strftime('%a, %d %b %Y %H:%M:%S GMT') if head_response.get('LastModified') else '',
                'ETag': head_response.get('ETag', ''),
                'Cache-Control': 'public, max-age=3600',
            }

            return StreamingResponse(
                generate(),
                status_code=206,  # Partial Content
                media_type=head_response.get('ContentType', 'application/octet-stream'),
                headers=headers
            )

        else:
            # No range request - return full content
            response = s3_client.get_object(
                Bucket=bucket,
                Key=path
            )

            def generate():
                for chunk in response['Body'].iter_chunks(chunk_size=65536):
                    yield chunk

            headers = {
                'Content-Type': response.get('ContentType', 'application/octet-stream'),
                'Content-Length': str(response.get('ContentLength', 0)),
                'Accept-Ranges': 'bytes',
                'Last-Modified': response.get('LastModified', '').strftime('%a, %d %b %Y %H:%M:%S GMT') if response.get('LastModified') else '',
                'ETag': response.get('ETag', ''),
                'Cache-Control': 'public, max-age=3600',
            }

            return StreamingResponse(
                generate(),
                media_type=response.get('ContentType', 'application/octet-stream'),
                headers=headers
            )

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            raise HTTPException(status_code=404, detail=f"Object not found: {bucket}/{path}")
        elif error_code in ['AccessDenied', '403']:
            raise HTTPException(status_code=403, detail=f"Access denied to {bucket}/{path}")
        else:
            raise HTTPException(status_code=500, detail=f"S3 error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Proxy error: {str(e)}")


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.environ.get('PORT', '8083')),
        log_level=os.environ.get('LOG_LEVEL', 'info').lower()
    )
