// Global type declarations for headless compatibility

declare global {
  interface Console {
    warn(...data: any[]): void
  }

  var console: Console

  // Web Streams API types for headless environments
  interface ReadableStream<R = any> {
    readonly locked: boolean
    getReader(): ReadableStreamDefaultReader<R>
  }

  interface ReadableStreamDefaultReader<R = any> {
    read(): Promise<ReadableStreamReadResult<R>>
    releaseLock(): void
    cancel(reason?: any): Promise<void>
  }

  interface ReadableStreamReadResult<T> {
    done: boolean
    value?: T
  }

  var ReadableStream: {
    prototype: ReadableStream
    new <R = any>(): ReadableStream<R>
  }

  // Abort API types for stream cancellation
  interface AbortSignal {
    readonly aborted: boolean
    addEventListener(type: string, listener: () => void): void
    removeEventListener(type: string, listener: () => void): void
  }

  interface AbortController {
    readonly signal: AbortSignal
    abort(): void
  }

  var AbortController: {
    prototype: AbortController
    new (): AbortController
  }

  // DOMException for abort error detection
  interface DOMException extends Error {
    readonly name: string
  }

  var DOMException: {
    prototype: DOMException
    new (message?: string, name?: string): DOMException
  }

  // Text encoding/decoding
  interface TextDecoder {
    decode(input?: BufferSource, options?: TextDecodeOptions): string
  }

  interface TextDecodeOptions {
    stream?: boolean
  }

  var TextDecoder: {
    prototype: TextDecoder
    new (label?: string, options?: TextDecoderOptions): TextDecoder
  }

  interface TextDecoderOptions {
    fatal?: boolean
    ignoreBOM?: boolean
  }
}

export {}
