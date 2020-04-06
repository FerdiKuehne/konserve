(ns konserve.filestore2
  (:require
   [clojure.java.io :as io]
   [konserve.serializers :as ser]
   [hasch.core :refer [uuid]]
   [clojure.data.fressian :as fress]
   [incognito.fressian :refer [incognito-read-handlers
                               incognito-write-handlers]]
   [konserve.protocols :refer [PEDNAsyncKeyValueStore
                               -exists? -get -get-meta -update-in -dissoc -assoc-in
                               PBinaryAsyncKeyValueStore -bget -bassoc
                               -serialize -deserialize
                               PKeyIterable]]
   [clojure.core.async :as async
    :refer [<!! <! >! timeout chan alt! go go-loop close! put!]])
  (:import
   [java.io
    InputStream
    DataInputStream DataOutputStream
    BufferedInputStream
    FileInputStream FileOutputStream
    ByteArrayOutputStream ByteArrayInputStream]
   [java.nio.channels Channels FileChannel AsynchronousFileChannel CompletionHandler ReadableByteChannel]
   [java.nio ByteBuffer]
   [java.nio.file Files StandardCopyOption FileSystems Path Paths OpenOption LinkOption
    StandardOpenOption]))

(defn- sync-folder [folder]
  (let [p (.getPath (FileSystems/getDefault) folder (into-array String []))
        fc (FileChannel/open p (into-array OpenOption []))]
    (.force fc true)
    (.close fc)))

(defn delete-store
  "Permanently deletes the folder of the store with all files."
  [folder]
  (let [f (io/file folder)
        parent-folder (.getParent f)]
    (doseq [c (.list (io/file folder))]
      (.delete (io/file (str folder "/" c))))
    (.delete f)
    (try
      (sync-folder parent-folder)
      (catch Exception e
        nil))))

(def
  ^{:doc "Type object for a Java primitive byte array."
    :private true
    }
  byte-array-type (class (make-array Byte/TYPE 0)))

(defmulti
  ^{:doc "Internal helper for write-binary"
    :private true
    :arglists '([input])}
  blob->channel
  (fn [input] [(type input)]))

(defmethod blob->channel [InputStream] [^InputStream input]
  (.getChannel input))

(defmethod blob->channel [byte-array-type] [^"[B" input]
  input)

(defn- completion-write-handler [ch msg]
  (proxy [CompletionHandler] []
    (completed [res att]
      (close! ch))
    (failed [t att]
      (put! ch (ex-info "Could not write key file."
                        (assoc msg :exception t)))
      (close! ch))))
                                        ;profile binary => timing ?,  resize the buffer *1000
                                        ;add error routine
                                        ;go-loop check error handling
;check binary for rewrite
(defn- write-binary [input-stream ac key start]
  (let [bis    (read-blob input-stream)
        stop   (+ 1024 start)
        buffer (ByteBuffer/allocate 1024)]
    (go
      (try
        (loop [start-byte start
               stop-byte  stop]
          (let [size   (.read bis buffer)
                _      (.flip buffer)
                res-ch (chan)]
            (when-not (= size -1)
              (.write ac buffer start-byte stop-byte (completion-write-handler res-ch {:type :write-binary-error
                                                                                 :key  key}))
              (<? res-ch)
              (.clear buffer)
              (recur (+ 1024 start-byte) (+ 1024 stop-byte)))))
           (catch Exception e
             (throw e))))))

(defn- write-edn [ac-new key serializer write-handlers start-byte value byte-array]
  (let [bos       (ByteArrayOutputStream.)
        _         (when byte-array (.write bos byte-array))
        _         (-serialize serializer bos write-handlers value)
        stop-byte (.size bos)
        buffer    (ByteBuffer/wrap (.toByteArray bos))
        _         (.close bos)
        result-ch (chan)]
    (.write ac-new buffer start-byte stop-byte (completion-write-handler result-ch {:type :write-edn-error
                                                                              :key  key}))
    result-ch))

(defn throw-if-exception
  "Helper method that checks if x is Exception and if yes, wraps it in a new
  exception, passing though ex-data if any, and throws it. The wrapping is done
  to maintain a full stack trace when jumping between multiple contexts."
  [x]
  (if (instance? #?(:clj Exception :cljs js/Error) x)
    (throw (ex-info (or #?(:clj (.getMessage ^Exception x)) (str x))
                    (or (ex-data x) {})
                    x))
    x))

(defmacro <?
  "Same as core.async <! but throws an exception if the channel returns a
  throwable error."
  [ch]
   `(throw-if-exception (async/<! ~ch)))

(defn- update-file [res-ch path fn serializer write-handlers [key & rkey] {:keys [up-fn up-fn-args up-fn-meta type config type input]} [old-meta old-value] ]
  (try
    (let [path-new             (Paths/get (str fn ".new") (into-array String []))
          standard-open-option (into-array StandardOpenOption
                                           [StandardOpenOption/WRITE
                                            StandardOpenOption/CREATE])
          ac-new               (AsynchronousFileChannel/open path-new standard-open-option)
          meta                 (apply up-fn-meta old-meta up-fn-args)
          value                (when (= type :edn)
                                 (if-not (empty? rkey)
                                   (apply update-in old-value rkey up-fn up-fn-args)
                                   (apply up-fn old-value up-fn-args)))
          bos                  (ByteArrayOutputStream.)
          _                    (-serialize serializer bos write-handlers meta)
          meta-size            (.size bos)
          _                    (.close bos)
          start-byte           (+ Long/BYTES meta-size)
          byte-array (byte-array Long/BYTES (list meta-size))]
        (go
          (try
            (<? (write-edn ac-new key serializer write-handlers 0 meta byte-array))
            (<? (if (= type :binary)
                  (write-binary input ac-new key start-byte)
                  (write-edn ac-new key serializer write-handlers start-byte value nil)))
            (Files/move path-new path
                        (into-array [StandardCopyOption/ATOMIC_MOVE]))
            (catch Exception e
              (put! res-ch (ex-info "Could not write key file."
                                    {:type      :write-key-error
                                     :key       key
                                     :exception e})))
            (finally
              (.close ac-new)
              (put! res-ch (if (= type :edn) [old-value value] true))
              (close! res-ch)))))))
;rename into read file

;refaktor completion handler for read

;read binary cut meta
(defn- read-file [ac res-ch path fn key-vec serializer write-handlers read-handlers {:keys [locked-cb type config operation format] :as env} meta-size ]
  (let [file-size (.size ac)
        {:keys [bb start-byte stop-byte]} (case format
                                            :meta {:bb         (ByteBuffer/allocate meta-size)
                                                   :start-byte Long/BYTES 
                                                   :stop-byte  (+ meta-size Long/BYTES)}
                                            :data {:bb         (ByteBuffer/allocate (if (= operation :read)
                                                                                      (- file-size meta-size Long/BYTES)
                                                                                      file-size))
                                                   :start-byte (if (= operation :read)
                                                                 (+ meta-size Long/BYTES)
                                                                 Long/BYTES)
                                                   :stop-byte  file-size})]
    (.read ac bb start-byte stop-byte )))


{:type :read-error
 :key  (first key-vec)}

(defn completion-read-handler [res-ch bb fn-read msg])
(proxy [CompletionHandler] []
  (completed [res att]
    (try
      (case operation
        :read  (if (= :binary type)
                 (try
                   (go
                     (>! res-ch (<! (locked-cb {:input-stream (ByteArrayInputStream. bb)
                                                :size         file-size
                                                :file         fn}))))
                   (catch Exception e
                     (ex-info "Could not read binary."
                              (assoc msg :exception e)))
                   (finally
                     (close! res-ch)))
                 (put! res-ch (-deserialize serializer read-handlers (ByteArrayInputStream. (.array bb)))))
        :write (let [array-buffer (.array bb)
                     old          (case type
                                    :edn    [(-deserialize serializer read-handlers (ByteArrayInputStream. array-buffer 0 meta-size))
                                             (-deserialize serializer read-handlers (ByteArrayInputStream. array-buffer meta-size file-size))]
                                    :binary [(-deserialize serializer read-handlers (ByteArrayInputStream. array-buffer))])]
                 (update-file res-ch path fn serializer write-handlers key-vec old env)))
      (catch Exception e
        (ex-info "Could not read key."
                 (assoc msg :exception e)))))
       (failed [t att]
         (throw
          (ex-info "Could not read key."
                   (assoc msg :exception t)))))

(defn completion-read-edn-handler [res-ch bb fn-read fn-write msg])
(let [array-buffer (.array bb)
      bais (if (= operation :write-edn)
             (fn [stb spb] (ByteArrayInputStream. array-buffer stb spb))
             (ByteArrayInputStream. array-buffer))]
  (case operation
    :read-edn     (put! res-ch (fn-read bais))
    :write-binary (fn-write [(fn-read bais)])
    :write-edn    (fn-write [(fn-read (bais 0 meta-size)) (fn-read (bais meta-size file-size))])))





(defn completion-read-meta-size-handler [res-ch bb fn-read operation msg]
  (proxy [CompletionHandler] []
    (completed [res att]
      (let [bais      (ByteArrayInputStream. (.array bb))
            meta-size (.read bais)
            _         (.close bais)]
        (try
          (if (nil? meta-size)
            (close! res-ch)
            (fn-read meta-size) )
          (catch Exception e
            (ex-info "Could not read key."
                     (accoc msg :exception e))))))
    (failed [t att]
      (put! res-ch (ex-info "Could not read key."
                            (accoc msg :exception e)))
      (.close ac))))

                                        ;add close ac channel after operation
;rename 
(defn- io-operation [key-vec folder serializer read-handlers write-handlers {:keys [operation] :as env}]
  (let [key          (first  key-vec)
        fn           (str folder "/" (uuid key))
        path         (Paths/get fn (into-array String []))
        res-ch       (chan)
        file-exists? (Files/exists path (into-array LinkOption []))]
    (if (or file-exists? (= :write-edn operation) (= :write-binary operation))
      (let [standard-open-option (if file-exists?
                                   (into-array StandardOpenOption [StandardOpenOption/READ])
                                   (into-array StandardOpenOption [StandardOpenOption/WRITE
                                                                   StandardOpenOption/CREATE]))
            ac                   (AsynchronousFileChannel/open path standard-open-option)]
        (if file-exists?
          (let [bb (ByteBuffer/allocate Long/BYTES)]
            (.read ac bb 0 Long/BYTES (completion-read-meta-size-handler bb (partial read-file ac res-ch path fn key-vec serializer write-handlers read-handlers env) {:type :read-error
                                                                                                                                                                       :key  key})))
          (update-file res-ch path fn serializer write-handlers key-vec env [nil nil])))
        (close! res-ch))
    res-ch))

(defrecord FileSystemStore [folder serializer read-handlers write-handlers locks config]
  PEDNAsyncKeyValueStore
  (-get [this key]
    (io-operation [key] folder serializer read-handlers write-handlers {:operation :read-edn
                                                                        :format    :data}))
  (-get-meta [this key]
    (io-operation [key] folder serializer read-handlers write-handlers {:operation :read-edn
                                                                        :format    :meta}))
  (-update-in [this key-vec meta-up up-fn] (-update-in this key-vec meta-up up-fn []))
  (-update-in [this key-vec meta-up up-fn args]
    (io-operation key-vec folder serializer read-handlers write-handlers {:operation  :write-edn
                                                                          :up-fn      up-fn
                                                                          :up-fn-args args
                                                                          :format     :data
                                                                          :up-fn-meta meta-up
                                                                          :config     config}))
  PBinaryAsyncKeyValueStore
  (-bget [this key locked-cb]
    (io-operation [key] folder serializer read-handlers write-handlers {:operation :read-binary
                                                                        :config    config
                                                                        :locked-cb locked-cb}))
  (-bassoc [this key meta-up input]
    (io-operation [key] folder serializer read-handlers write-handlers {:operation  :write-binary
                                                                        :input      input
                                                                        :up-fn-meta meta-up
                                                                        :config     config})))

(defn- check-and-create-folder [path]
    (let [f         (io/file path)
          test-file (io/file (str path "/" (java.util.UUID/randomUUID)))]
      (when-not (.exists f)
      (.mkdir f))
    (when-not (.createNewFile test-file)
      (throw (ex-info "Cannot write to folder." {:type   :not-writable
                                                 :folder path})))
    (.delete test-file)))

(defn new-fs-store
  "Filestore contains a Key and a Data Folder"
  [path  & {:keys [serializer read-handlers write-handlers config]
            :or   {serializer     (ser/fressian-serializer)
                   read-handlers  (atom {})
                   write-handlers (atom {})
                   config         {:fsync true}}}]
  (let [_     (check-and-create-folder path)
        store (map->FileSystemStore {:folder         path
                                     :serializer     serializer
                                     :read-handlers  read-handlers
                                     :write-handlers write-handlers
                                     :locks          (atom {})
                                     :config         config})]
    (go store)))

(comment

  (do
    (delete-store "/tmp/konserve")
    (def store (<!! (new-fs-store "/tmp/konserve")))
    (<!! (-update-in store ["fooNew2"] (fn [old] {:format :edn :key "bar2"}) (fn [old] 0))))

  (<!! (-update-in store ["fooNew2"] (fn [old] {:format :edn :key "bar2"}) inc))

  (<!! (-get store "fooNew2"))

  (<!! (-bassoc store :foobar9 (fn [old] {:format :binary :key :binbar}) (FileInputStream. "psheet.pdf")))

  (<!! (-bget store :binbar (fn [{:keys [input-stream]}]
                             (go
                               (do
                                 (prn (map byte (slurp input-stream)))
                                 true)))))

  (<!! (-get-meta store :foobar9))


  (<!! (-bassoc store :binaryfoo (fn [old] {:format :binary :key :binaryfoo}) (byte-array (range 10))))



  (read-blob "1232")

  (type (byte-array (range 10)))

                                        ;using Channel Method
(defn copy-file-using-channel [file-source file-dest]
  (let [fci         (.getChannel file-source)
        fco         (.getChannel file-dest)
        byte-buffer (ByteBuffer/allocate 1024)]
    (while (not (= -1 (.read fci buffer)))
      (.flip buffer)
      (.write fco buffer)
      (.clear buffer))))
(copy-file-using-channel (FileInputStream. "psheet.pdf") (FileOutputStream. "yolo"))

                                        ;java.nio.channels.ReadableByteChannel, java.nio.channels.WritableByteChannel
(defn copy-file-using-byte-channel [file-source file-dest]
  (let [fci         (.getChannel file-source)
        fco         (.getChannel file-dest)
        byte-buffer (ByteBuffer/allocateDirect 1024)]
    (while (not (= -1 (.read fci buffer)))
      (.flip buffer)
      (.write fco buffer)
      (.compact buffer))))

)


