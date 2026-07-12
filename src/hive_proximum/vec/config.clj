;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-proximum.vec.config
  "Typed config for the Proximum HNSW vector store, resolved via hive-di
   defconfig. Mirrors the DatalevinKGConfig surface so a vec slot's
   backend swap is config-driven, not code-driven.

   Resolution per field (via hive-di coalesce):
     1. Explicit override map (caller passes :db-path / :dim)
     2. HIVE_VEC_PROXIMUM_PATH / HIVE_VEC_PROXIMUM_DIM env var
     3. ~/.config/hive-mcp/config.edn at [:services :proximum <key>]
     4. Hardcoded XDG default

   Lives in hive-proximum (the consumer of the proximum HNSW lib);
   hive-mcp depends on hive-proximum and late-binds via
   `hive-mcp.knowledge-graph.slots.factory/backend->store :proximum`."
  (:require [hive-di.core :as di]
            [hive-di.source :as src]))

(def ^:const config-edn-path
  "Canonical hive-mcp config file. Each field's :file source reads from
   here. Symmetric with DatalevinKGConfig and DatahikeKGConfig."
  (str (System/getProperty "user.home") "/.config/hive-mcp/config.edn"))

(def ^:const default-db-path
  "XDG-conformant default. Sibling subdir to the Datalevin / Datahike
   stores so operators can keep all backends side-by-side during the
   STORAGE migration."
  (str (System/getProperty "user.home") "/.local/share/hive-mcp/proximum"))

(def ^:const default-dim
  "Default embedding dimensionality. Matches sentence-transformers
   `all-MiniLM-L6-v2` (384) — the small-model fallback. Operators wiring
   in a larger embedder (Qwen3 4096, OpenAI 1536) override via env or
   config.edn :services :proximum :dim."
  384)

(def ^:const default-capacity
  "Default HNSW index capacity (max vector count). proximum's library
   default is 10M, which * dim * 4B overflows Integer.MAX_VALUE on the
   mmap backing file (capacity * dim * 4 must fit in 2GiB). 100_000
   keeps the mmap under 200MB at dim=384 and is plenty for the carto
   snippet / memory entry slots in early Phase 2.

   Operators wiring in a production-scale workload override via env
   (HIVE_VEC_PROXIMUM_CAPACITY) or config.edn :services :proximum
   :capacity. Capacity is fixed at index creation — bumping it requires
   creating a new index and re-inserting (proximum has no in-place
   resize)."
  100000)

(di/defconfig ProximumVecConfig
  :db-path (src/coalesce
             [(src/env "HIVE_VEC_PROXIMUM_PATH" :required false)
              (src/file config-edn-path [:services :proximum :path]
                        :required false)]
             :default default-db-path
             :type :string
             :doc "Proximum Konserve filestore directory. Override via env or config.edn :services :proximum :path.")
  :dim (src/coalesce
         [(src/env "HIVE_VEC_PROXIMUM_DIM" :required false)
          (src/file config-edn-path [:services :proximum :dim]
                    :required false)]
         :default default-dim
         :type :long
         :doc "Vector dimensionality for the Proximum HNSW index. Override via env or config.edn :services :proximum :dim.")
  :capacity (src/coalesce
              [(src/env "HIVE_VEC_PROXIMUM_CAPACITY" :required false)
               (src/file config-edn-path [:services :proximum :capacity]
                         :required false)]
              :default default-capacity
              :type :long
              :doc "Max vector count the HNSW index can hold. capacity * dim * 4B must fit in Integer.MAX_VALUE for the mmap-backed file. Override via env or config.edn :services :proximum :capacity."))
