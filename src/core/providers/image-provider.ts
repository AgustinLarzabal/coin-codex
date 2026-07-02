import type { SourceConfig } from "../source-config.js";

export type ImageProviderPayload = Record<string, unknown>;

export type DownloadImageInput = {
  sourceConfig: SourceConfig;
  imageUrl: string;
};

export type DownloadImageResult = {
  contentType: string | null;
  content: Uint8Array;
  providerPayload: ImageProviderPayload;
};

export type ImageProviderErrorDetails = {
  code: string;
  retryable: boolean;
  statusCode?: number;
  providerPayload?: ImageProviderPayload;
};

export class ImageProviderError extends Error {
  constructor(message: string, readonly details: ImageProviderErrorDetails) {
    super(message);
  }
}

export interface ImageProvider {
  downloadImage(input: DownloadImageInput): Promise<DownloadImageResult>;
}

export class HttpImageProvider implements ImageProvider {
  async downloadImage(input: DownloadImageInput): Promise<DownloadImageResult> {
    const response = await fetch(input.imageUrl);
    if (!response.ok) {
      throw new ImageProviderError(`image download failed: ${response.status}`, {
        code: "IMAGE_DOWNLOAD_FAILED",
        retryable: response.status >= 500 || response.status === 429,
        statusCode: response.status,
        providerPayload: {
          contentType: response.headers.get("content-type"),
        },
      });
    }

    return {
      contentType: response.headers.get("content-type"),
      content: new Uint8Array(await response.arrayBuffer()),
      providerPayload: {
        adapter: "http",
        contentLength: response.headers.get("content-length"),
      },
    };
  }
}
