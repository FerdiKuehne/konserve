(ns konserve.serializers
  (:require [konserve.protocols :refer [PStoreSerializer -serialize -deserialize]]
            #?@(:clj [[clojure.data.fressian :as fress]
                      [incognito.fressian :refer [incognito-read-handlers
                                                  incognito-write-handlers]]
                      [taoensso.nippy :as nippy]])
            #?@(:cljs [[fress.api :as fress]
                       [incognito.fressian :refer [incognito-read-handlers incognito-write-handlers]]])
            [incognito.edn :refer [read-string-safe]])
  #?(:clj (:import [java.io FileOutputStream FileInputStream DataInputStream DataOutputStream]
                   [org.fressian Reader]
                   [org.fressian.handlers WriteHandler ReadHandler]))
  (:import
   [java.io ByteArrayInputStream ByteArrayOutputStream DataInputStream
    DataOutputStream Serializable ObjectOutputStream ObjectInputStream
    DataOutput DataInput]
   [java.lang.reflect Method]
   ;; [java.net URI] ; TODO
   [java.util Date UUID]
   [java.util.regex Pattern]
   [clojure.lang Keyword Symbol BigInt Ratio
    APersistentMap APersistentVector APersistentSet
    IPersistentMap ; IPersistentVector IPersistentSet IPersistentList
    PersistentQueue PersistentTreeMap PersistentTreeSet PersistentList
    LazySeq IRecord ISeq]))


#?(:clj
   (defrecord FressianSerializer [custom-read-handlers custom-write-handlers]
     PStoreSerializer
     (-deserialize [_ read-handlers bytes]
       (fress/read bytes
                   :handlers (-> (merge fress/clojure-read-handlers
                                        custom-read-handlers
                                        (incognito-read-handlers read-handlers))
                                 fress/associative-lookup)))

     (-serialize [_ bytes write-handlers val]
       (let [w (fress/create-writer bytes :handlers (-> (merge
                                                         fress/clojure-write-handlers
                                                         custom-write-handlers
                                                         (incognito-write-handlers write-handlers))
                                                        fress/associative-lookup
                                                        fress/inheritance-lookup))]
         (fress/write-object w val)))))

#?(:cljs
   (defrecord FressianSerializer [custom-read-handlers custom-write-handlers]
     PStoreSerializer
     (-deserialize [_ read-handlers bytes]
       (let [buf->arr (.from js/Array (.from js/Int8Array bytes))
             buf      (fress.impl.buffer/BytesOutputStream. buf->arr (count buf->arr))
             reader   (fress/create-reader buf
                                           :handlers (merge custom-read-handlers
                                                            (incognito-read-handlers read-handlers)))
             read     (fress/read-object reader)]
         read))
     (-serialize [_ bytes write-handlers val]
       (let [writer (fress/create-writer bytes
                                         :handlers (merge
                                                    custom-write-handlers
                                                    (incognito-write-handlers write-handlers)))]
         (fress/write-object writer val)))))

(defn fressian-serializer
  ([] (fressian-serializer {} {}))
  ([read-handlers write-handlers] (map->FressianSerializer {:custom-read-handlers read-handlers
                                                            :custom-write-handlers write-handlers})))

(defrecord StringSerializer []
  PStoreSerializer
  (-deserialize [_ read-handlers s]
    (read-string-safe @read-handlers s))
  (-serialize [_ output-stream _ val]
    #?(:clj
       (binding [clojure.core/*out* output-stream]
         (pr val)))
    #?(:cljs
         (pr-str val))))

(defn string-serializer []
  (map->StringSerializer {}))

(defrecord NippySerializer []
  PStoreSerializer
  (-deserialize [_ _ bytes]
    (do
      
      (nippy/thaw-from-in! bytes)))
  (-serialize [_ bytes _ val]
    (nippy/freeze-to-out! bytes val)))

(defn nippy-serializer []
  (map->NippySerializer {}))





