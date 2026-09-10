"use client";

import { useState, useEffect, useRef, useCallback } from "react";

export interface PollingOptions<T> {
  fetcher: () => Promise<T>;
  intervalMs?: number;
  getId?: (item: any) => string;
  getStateSig?: (item: any) => string;
}

export function usePolling<T>({
  fetcher,
  intervalMs = 3000,
  getId,
  getStateSig,
}: PollingOptions<T>) {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [changedKeys, setChangedKeys] = useState<Set<string>>(new Set());

  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;
  const getIdRef = useRef(getId);
  getIdRef.current = getId;
  const getStateSigRef = useRef(getStateSig);
  getStateSigRef.current = getStateSig;

  const previousSignatures = useRef<Map<string, string>>(new Map());
  const changeTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  const loadData = useCallback(async () => {
    try {
      const result = await fetcherRef.current();
      setData(result);
      setError(null);
      setLastUpdated(new Date());

      const getIdentifier = getIdRef.current;
      const getSignature = getStateSigRef.current;
      if (Array.isArray(result) && getIdentifier && getSignature) {
        const newlyChanged = new Set<string>();
        for (const item of result) {
          const id = getIdentifier(item);
          const sig = getSignature(item);
          const prevSig = previousSignatures.current.get(id);
          if (prevSig !== undefined && prevSig !== sig) {
            newlyChanged.add(id);
          }
          previousSignatures.current.set(id, sig);
        }

        if (newlyChanged.size > 0) {
          setChangedKeys(newlyChanged);
          if (changeTimeoutRef.current) clearTimeout(changeTimeoutRef.current);
          changeTimeoutRef.current = setTimeout(() => {
            setChangedKeys(new Set());
          }, 1500);
        }
      }
    } catch (err: any) {
      setError(err?.message || "Connection error to control-plane");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadData();

    const interval = setInterval(() => {
      if (typeof document !== "undefined" && document.hidden) {
        return;
      }
      loadData();
    }, intervalMs);

    return () => {
      clearInterval(interval);
      if (changeTimeoutRef.current) clearTimeout(changeTimeoutRef.current);
    };
  }, [loadData, intervalMs]);

  return {
    data,
    loading,
    error,
    lastUpdated,
    changedKeys,
    refresh: loadData,
  };
}
