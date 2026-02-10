import { useRef, useEffect, useSyncExternalStore } from 'react'
import { MessageBinaryFormat } from '../types'
import * as jsondiffpatch from 'jsondiffpatch'

const jdf = jsondiffpatch.create({})

// Exact copy of the patch function from v0/chat/lib/diffpatch.ts
function patch(original: any, delta: any) {
  const newObj = jdf.clone(original)

  // Check for our customized delta
  if (Array.isArray(delta) && delta[1] === 9 && delta[2] === 9) {
    // Get the path to the modified element
    const indexes = delta[0].slice(0, -1)
    // Get the string to be appended
    const value = delta[0].slice(-1)
    let obj = newObj as any
    for (const index of indexes) {
      if (typeof obj[index] === 'string') {
        obj[index] += value
        return newObj
      }
      obj = obj[index]
    }
  }

  // If not custom delta, apply standard jsondiffpatch-ing
  jdf.patch(newObj, delta)
  return newObj
}

export interface StreamingMessageState {
  content: MessageBinaryFormat
  isStreaming: boolean
  error?: string
  isComplete: boolean
}

export interface UseStreamingMessageOptions {
  onChunk?: (chunk: MessageBinaryFormat) => void
  onComplete?: (finalContent: MessageBinaryFormat) => void
  onError?: (error: string) => void
  onChatData?: (chatData: any) => void
}

/**
 * Simple cancellation token that avoids the need for DOM AbortController types.
 */
class CancelToken {
  private _cancelled = false

  get cancelled(): boolean {
    return this._cancelled
  }

  cancel(): void {
    this._cancelled = true
  }
}

// Stream state manager - isolated from React lifecycle
class StreamStateManager {
  private content: MessageBinaryFormat = []
  private isStreaming: boolean = false
  private error?: string
  private isComplete: boolean = false
  private callbacks = new Set<() => void>()
  private cachedState: StreamingMessageState | null = null
  private activeStream: ReadableStream<Uint8Array> | null = null
  private cancelToken: CancelToken | null = null

  subscribe = (callback: () => void) => {
    this.callbacks.add(callback)
    return () => {
      this.callbacks.delete(callback)
    }
  }

  private notifySubscribers = () => {
    // Invalidate cached state when state changes
    this.cachedState = null
    this.callbacks.forEach((callback) => callback())
  }

  getState = (): StreamingMessageState => {
    // Return cached state to prevent infinite re-renders
    if (this.cachedState === null) {
      this.cachedState = {
        content: this.content,
        isStreaming: this.isStreaming,
        error: this.error,
        isComplete: this.isComplete,
      }
    }
    return this.cachedState
  }

  processStream = async (
    stream: ReadableStream<Uint8Array>,
    options: UseStreamingMessageOptions = {},
  ): Promise<void> => {
    // If this exact stream is already being actively processed, skip
    if (this.activeStream === stream) {
      return
    }

    // Cancel any in-progress stream read before starting a new one
    this.abort()

    // Handle locked streams with an error state so the UI is not stuck
    if (stream.locked) {
      this.setError(
        'Stream is locked and cannot be processed. Please try sending your message again.',
      )
      return
    }

    this.activeStream = stream
    const token = new CancelToken()
    this.cancelToken = token

    this.reset()
    this.setStreaming(true)

    try {
      await this.readStream(stream, options, token)
    } catch (err) {
      // Don't report errors from intentional cancellations
      if (token.cancelled) {
        return
      }
      const errorMessage =
        err instanceof Error ? err.message : 'Unknown streaming error'
      this.setError(errorMessage)
      options.onError?.(errorMessage)
    } finally {
      if (!token.cancelled) {
        this.setStreaming(false)
        this.activeStream = null
      }
    }
  }

  /**
   * Cancel any in-progress stream reading. Safe to call multiple times.
   */
  abort = () => {
    if (this.cancelToken) {
      this.cancelToken.cancel()
      this.cancelToken = null
    }
    this.activeStream = null
  }

  private reset = () => {
    this.content = []
    this.isStreaming = false
    this.error = undefined
    this.isComplete = false
    this.notifySubscribers()
  }

  private setStreaming = (streaming: boolean) => {
    this.isStreaming = streaming
    this.notifySubscribers()
  }

  private setError = (error: string) => {
    this.error = error
    this.isStreaming = false
    this.isComplete = false
    this.notifySubscribers()
  }

  private setComplete = (complete: boolean) => {
    this.isComplete = complete
    this.notifySubscribers()
  }

  private updateContent = (newContent: MessageBinaryFormat) => {
    this.content = [...newContent]
    this.notifySubscribers()
  }

  private readStream = async (
    stream: ReadableStream<Uint8Array>,
    options: UseStreamingMessageOptions,
    token: CancelToken,
  ): Promise<void> => {
    const reader = stream.getReader()
    const decoder = new TextDecoder()
    let buffer = ''
    let currentContent: MessageBinaryFormat = []

    try {
      while (true) {
        // Check for cancellation before each read
        if (token.cancelled) {
          return
        }

        const { done, value } = await reader.read()
        if (done) {
          break
        }

        // Check for cancellation after each read
        if (token.cancelled) {
          return
        }

        const chunk = decoder.decode(value, { stream: true })
        buffer += chunk
        const lines = buffer.split('\n')
        buffer = lines.pop() || ''

        for (const line of lines) {
          if (line.trim() === '') {
            continue
          }

          // Handle SSE format (data: ...)
          let jsonData: string
          if (line.startsWith('data: ')) {
            jsonData = line.slice(6) // Remove "data: " prefix
            if (jsonData === '[DONE]') {
              this.setComplete(true)
              options.onComplete?.(currentContent)
              return
            }
          } else {
            // Handle raw JSON lines (fallback)
            jsonData = line
          }

          try {
            // Parse the JSON data
            const parsedData = JSON.parse(jsonData)

            // Handle v0 streaming format
            if (parsedData.type === 'connected') {
              continue
            } else if (parsedData.type === 'done') {
              this.setComplete(true)
              options.onComplete?.(currentContent)
              return
            } else if (
              parsedData.object &&
              parsedData.object.startsWith('chat')
            ) {
              // Handle chat metadata messages (chat, chat.title, chat.name, etc.)
              options.onChatData?.(parsedData)
              continue
            } else if (parsedData.delta) {
              // Apply the delta using jsondiffpatch
              const patchedContent = patch(currentContent, parsedData.delta)
              currentContent = Array.isArray(patchedContent)
                ? (patchedContent as MessageBinaryFormat)
                : []

              this.updateContent(currentContent)
              options.onChunk?.(currentContent)
            }
          } catch (e) {
            console.warn('Failed to parse streaming data:', line, e)
          }
        }
      }

      if (!token.cancelled) {
        this.setComplete(true)
        options.onComplete?.(currentContent)
      }
    } finally {
      reader.releaseLock()
    }
  }
}

/**
 * Hook for handling streaming message content from v0 API using useSyncExternalStore
 */
export function useStreamingMessage(
  stream: ReadableStream<Uint8Array> | null,
  options: UseStreamingMessageOptions = {},
): StreamingMessageState {
  // Create a stable stream manager instance
  const managerRef = useRef<StreamStateManager | null>(null)
  if (!managerRef.current) {
    managerRef.current = new StreamStateManager()
  }

  const manager = managerRef.current

  // Keep a stable ref to options so the useEffect does not re-fire on every render
  const optionsRef = useRef(options)
  optionsRef.current = options

  // Subscribe to state changes using useSyncExternalStore
  const state = useSyncExternalStore(
    manager.subscribe,
    manager.getState,
    manager.getState,
  )

  // Process stream in useEffect (not during render) to avoid React Strict Mode
  // double-invocation issues and to properly handle cleanup/abort
  useEffect(() => {
    if (stream) {
      manager.processStream(stream, optionsRef.current)
    }

    return () => {
      // Cancel in-progress reads when stream changes or component unmounts
      manager.abort()
    }
  }, [stream, manager])

  return state
}
