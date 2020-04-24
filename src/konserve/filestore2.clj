(ns konserve.filestore2
  (:require
   [clojure.java.io :as io]
   [konserve.serializers :as ser]
   [hasch.core :refer [uuid]]
   [clojure.data.fressian :as fress]
   [incognito.fressian :refer [incognito-read-handlers
                               incognito-write-handlers]]
   [konserve.protocols :refer [PEDNAsyncKeyValueStore
                               -get -get-meta -update-in -dissoc -assoc-in
                               PBinaryAsyncKeyValueStore -bget -bassoc
                               -serialize -deserialize
                               PKeyIterable
                               -keys]]
   [konserve.utils :refer [go-try <? throw-if-exception]]
   [clojure.core.async :as async
    :refer [<!! <! >! timeout chan alt! go go-loop close! put!]])
  (:import
   [java.io
    Reader
    File
    InputStream
    StringReader
    CharArrayReader
    DataInputStream DataOutputStream
    BufferedInputStream
    FileInputStream FileOutputStream
    ByteArrayOutputStream ByteArrayInputStream]
   [java.nio.channels Channels FileChannel AsynchronousFileChannel CompletionHandler ReadableByteChannel]
   [java.nio
    ByteBuffer
    CharBuffer]
   [java.nio.file Files StandardCopyOption FileSystems Path Paths OpenOption LinkOption
    StandardOpenOption FileVisitOption]))

(defn- sync-folder [folder]
  (let [p (.getPath (FileSystems/getDefault) folder (into-array String []))
        fc (FileChannel/open p (into-array OpenOption []))]
    (.force fc true)
    (.close fc)))

(defn delete-store
  "Permanently deletes the folder of the store with all files."
  [folder]
  (let [f             (io/file folder)
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

(def
  ^{:doc "Type object for a Java primitive char array."
    :private true}
  char-array-type (class (make-array Character/TYPE 0)))

(defmulti
  ^{:doc "Internal helper for write-binary"
    :private true
    :arglists '([input buffer-size])}
  blob->channel
  (fn [input buffer-size] [(type input)]))

(defmethod blob->channel [InputStream] [^InputStream input buffer-size]
  [(.getChannel input)
   (ByteBuffer/allocate buffer-size)
   (fn [bis buffer] (.read bis buffer))])

(defmethod blob->channel [byte-array-type] [^"[B" input buffer-size]
  [(Channels/newChannel (ByteArrayInputStream. input))
   (ByteBuffer/allocate buffer-size)
   (fn [bis buffer] (.read bis buffer))])

(defmethod blob->channel [File] [^File input buffer-size]
  [(Channels/newChannel (FileInputStream. input))
   (ByteBuffer/allocate buffer-size)
   (fn [bis buffer] (.read bis buffer))])

(defmethod blob->channel [String] [^String input buffer-size]
  [(Channels/newChannel (ByteArrayInputStream. (.getBytes input)))
   (ByteBuffer/allocate buffer-size)
   (fn [bis buffer] (.read bis buffer))])

(defmethod blob->channel [char-array-type] [input buffer-size]
  [(Channels/newChannel (ByteArrayInputStream. char-array-type))
   (ByteBuffer/allocate buffer-size)
   (fn [bis buffer] (.read bis buffer))])
;remove bytebuffer ;move char-array to vector;url to reader;cut array; typ readerffno;update resource 
(defmethod blob->channel [Reader] [^Reader input buffer-size]
  [input
     (ByteBuffer/allocate buffer-size)
     (fn [bis nio-buffer]
       (let [char-array (make-array Character/TYPE buffer-size)
             size (.read bis char-array)]
           (try
             (when-not (= size -1)
               (let [char-array (java.util.Arrays/copyOf char-array size)]
                 (.put nio-buffer (.getBytes (String. char-array)))))
             size
             (catch Exception e
              (throw e)))))])

                                        ;config sync / async
;(<!! (-bassoc store :binaryfoo (fn [old] {:format :binary :key :binaryfoo}) (StringReader. "foo")))

#_(<!! (-bget store :binaryfoo (fn [{:keys [input-stream]}]
                               (go
                                 (do
                                   (prn (slurp input-stream)) 
                                   true)))))
;refactor blob channel examples
(defn- completion-write-handler [ch msg]
  (proxy [CompletionHandler] []
    (completed [res att]
      (close! ch))
    (failed [t att]
      (put! ch (ex-info "Could not write key file."
                        (assoc msg :exception t)))
      (close! ch))))

(defn- write-binary [input-stream ac buffer-size key start]
  (let [[bis buffer read]    (blob->channel input-stream buffer-size)
        stop   (+ buffer-size start)]
    (go-try
        (loop [start-byte start
               stop-byte  stop]
          (let [size   (read bis buffer)
                _      (.flip buffer)
                res-ch (chan)]
            (if (= size -1)
              (close! res-ch)
              (do (.write ac buffer start-byte stop-byte (completion-write-handler res-ch {:type :write-binary-error
                                                                                           :key  key}))
                  (<? res-ch)
                  (.clear buffer)
                  (recur (+ buffer-size start-byte) (+ buffer-size stop-byte))))))
        (finally
          (.close bis)
          (.clear buffer)))))

(defn- write-edn [ac-new key serializer write-handlers start-byte value byte-array]
  (let [bos       (ByteArrayOutputStream.)
        _         (when byte-array  (.write bos byte-array))
        _         (-serialize serializer bos write-handlers value)
        stop-byte (.size bos)
        buffer    (ByteBuffer/wrap (.toByteArray bos))
        result-ch (chan)]
    (go-try
        (.write ac-new buffer start-byte stop-byte (completion-write-handler result-ch {:type :write-edn-error
                                                                                        :key  key}))
        (<? result-ch)
        (finally
          (close! result-ch)
          (.clear buffer)
          (.close bos)))))

(defn- update-file [folder config path serializer write-handlers buffer-size [key & rkey] {:keys [file-name up-fn up-fn-args up-fn-meta type config operation input msg]} [old-meta old-value]]
  (try
    (let [path-new             (Paths/get (str file-name ".new") (into-array String []))
          standard-open-option (into-array StandardOpenOption
                                           [StandardOpenOption/WRITE
                                            StandardOpenOption/CREATE])
          ac-new               (AsynchronousFileChannel/open path-new standard-open-option)
          meta                 (apply up-fn-meta old-meta up-fn-args)
          value                (when (= operation :write-edn)
                                 (if-not (empty? rkey)
                                   (apply update-in old-value rkey up-fn up-fn-args)
                                   (apply up-fn old-value up-fn-args)))
          bos                  (ByteArrayOutputStream.)
          _                    (-serialize serializer bos write-handlers meta)
          meta-size            (.size bos)
          _                    (.close bos)
          start-byte           (+ Long/BYTES meta-size)
          byte-array           (byte-array Long/BYTES (list meta-size))]
      (go-try
          (<? (write-edn ac-new key serializer write-handlers 0 meta byte-array))
          (<? (if (= operation :write-binary)
                (write-binary input ac-new buffer-size key start-byte)
                (write-edn ac-new key serializer write-handlers start-byte value nil)))
          (.close ac-new) ;add config fileesync
          (Files/move path-new path
                      (into-array [StandardCopyOption/ATOMIC_MOVE]))
          (when (:fsync config)
            (sync-folder folder))
          (if (= operation :write-edn) [old-value value] true)
          (finally
            (.close ac-new))))))

(defn completion-read-handler [res-ch bb meta-size file-size {:keys [msg operation locked-cb file-name]} fn-read]
  (proxy [CompletionHandler] []
    (completed [res att]
      (try
        (case operation
          (:read-edn :read-meta) (let [bais-read (ByteArrayInputStream. (.array bb))
                                       value     (fn-read bais-read)
                                       _         (.close bais-read)]
                                   (put! res-ch value))
          :write-binary          (let [bais-read (ByteArrayInputStream. (.array bb))
                                       value     (fn-read bais-read)
                                       _         (.close bais-read)]
                                   (put! res-ch [value nil]))
          :write-edn             (let [bais-meta  (ByteArrayInputStream. (.array bb) 0 meta-size)
                                       meta       (fn-read bais-meta)
                                       _          (.close bais-meta)
                                       bais-value (ByteArrayInputStream. (.array bb) meta-size file-size)
                                       value      (fn-read bais-value)
                                       _          (.close bais-value)]
                                   (put! res-ch [meta value]))
          :read-binary           (go (>! res-ch (<! (locked-cb {:input-stream (ByteArrayInputStream. (.array bb))
                                                                :size         file-size
                                                                :file         file-name})))))
          (catch Exception e
            (put! res-ch (ex-info "Could not read key."
                            (assoc msg :exception e))))))
    (failed [t att]
      (put! res-ch (ex-info "Could not read key."
                            (assoc msg :exception t))))))

(defn- read-file [ac serializer read-handlers {:keys [operation] :as env} meta-size]
  (let [file-size                         (.size ac)
        {:keys [bb start-byte stop-byte]} (cond
                                            (= operation :write-edn)                                   {:bb         (ByteBuffer/allocate file-size)
                                                                                                        :start-byte Long/BYTES
                                                                                                        :stop-byte  file-size}
                                            (or (= operation :read-meta)  (= operation :write-binary)) {:bb         (ByteBuffer/allocate meta-size)
                                                                                                        :start-byte Long/BYTES 
                                                                                                        :stop-byte  (+ meta-size Long/BYTES)}
                                            :else                                                      {:bb         (ByteBuffer/allocate (- file-size meta-size Long/BYTES))
                                                                                                        :start-byte (+ meta-size Long/BYTES)
                                                                                                        :stop-byte  file-size})
        res-ch (chan)]
    (go-try
      (.read ac bb start-byte stop-byte
          (completion-read-handler res-ch bb meta-size file-size env
                                   (partial -deserialize serializer read-handlers)))
      (<? res-ch)
      (finally
        (.clear bb)))))

(defn completion-read-meta-size-handler [res-ch bb msg]
  (proxy [CompletionHandler] []
    (completed [res att]
      (let [bais      (ByteArrayInputStream. (.array bb))
            meta-size (.read bais)
            _         (.close bais)]
        (try
          (if (nil? meta-size)
            (close! res-ch)
            (put! res-ch meta-size))
          (catch Exception e
            (ex-info "Could not read key."
                     (assoc msg :exception e))))))
    (failed [t att]
      (put! res-ch (ex-info "Could not read key."
                            (assoc msg :exception t))))))

(defn list-keys [folder serializer read-handlers]
  (let [path       (Paths/get folder (into-array String []))
        file-paths (Files/newDirectoryStream path)]
    (->> (map
          #(when (Files/exists % (into-array LinkOption []))
             (let [ac        (AsynchronousFileChannel/open % (into-array StandardOpenOption [StandardOpenOption/READ]))
                   bb        (ByteBuffer/allocate Long/BYTES)
                   path-name (.toString %)
                   env       {:operation :read-meta
                              :msg       {:type :read-meta-error
                                          :path path-name}}
                   res-ch    (chan)]
               (go-try
                   (.read ac bb 0 Long/BYTES
                          (completion-read-meta-size-handler res-ch bb
                                                             {:type :read-list-keys
                                                              :path path-name}))
                 (<? (read-file ac serializer read-handlers env (<? res-ch)))
                 (finally
                   (.clear bb)
                   (.close ac)
                   (close! res-ch)))))
          file-paths)
         async/merge
         (async/into #{}))))

(defn- io-operation [key-vec folder config serializer read-handlers write-handlers buffer-size {:keys [operation msg] :as env}]
  (let [key          (first  key-vec)
        fn           (str folder "/" (uuid key))
        env          (assoc env :file-name fn)
        path         (Paths/get fn (into-array String []))
        file-exists? (Files/exists path (into-array LinkOption []))]
    (if (or file-exists? (= :write-edn operation) (= :write-binary operation))
      (let [standard-open-option (if file-exists?
                                   (into-array StandardOpenOption [StandardOpenOption/READ])
                                   (into-array StandardOpenOption [StandardOpenOption/WRITE
                                                                   StandardOpenOption/CREATE]))
            ac                   (AsynchronousFileChannel/open path standard-open-option)]
        (if file-exists?
          (let [bb (ByteBuffer/allocate Long/BYTES)
                res-ch (chan)]
            (go-try
               (.read ac bb 0 Long/BYTES
                      (completion-read-meta-size-handler res-ch bb
                                                         {:type :read-meta-size-error
                                                          :key  key}))
               (let [meta-size (<? res-ch)
                     old       (<? (read-file ac serializer read-handlers env meta-size))]
                 (if (or (= :write-edn operation) (= :write-binary operation))
                   (<? (update-file folder config path serializer write-handlers buffer-size key-vec env old))
                   old))
               (finally
                 (close! res-ch)
                 (.clear bb)
                 (.close ac))))
          (go-try
              (<? (update-file folder config path serializer write-handlers buffer-size key-vec env [nil nil]))
              (finally
                (.close ac)))))
      (go nil))))

(defn- delete-file [key folder]
  (let [fn           (str folder "/" (uuid key))
        path         (Paths/get fn (into-array String []))
        res-ch       (chan)
        file-exists? (Files/exists path (into-array LinkOption []))]
    (if file-exists?
      (do (Files/delete path) 
          (put! res-ch true)
          (close! res-ch))
      (close! res-ch))
    res-ch))

(defrecord FileSystemStore [folder serializer read-handlers write-handlers buffer-size locks config]
  PEDNAsyncKeyValueStore
  (-get [this key]
    (io-operation [key] folder config serializer read-handlers write-handlers buffer-size {:operation :read-edn
                                                                                    :format    :data
                                                                                    :msg       {:type :read-edn-error
                                                                                                :key  key}}))
  (-get-meta [this key]
    (io-operation [key] folder config serializer read-handlers write-handlers buffer-size {:operation :read-meta
                                                                                    :msg       {:type :read-meta-error
                                                                                                :key  key}}))
  (-update-in [this key-vec meta-up up-fn] (-update-in this key-vec meta-up up-fn []))
  (-update-in [this key-vec meta-up up-fn args]
    (io-operation key-vec folder config serializer read-handlers write-handlers buffer-size {:operation  :write-edn
                                                                                      :up-fn      up-fn
                                                                                      :up-fn-args args
                                                                                      :up-fn-meta meta-up
                                                                                      :config     config
                                                                                      :msg        {:type :write-edn-error
                                                                                                   :key  (first key-vec)}}))
  (-dissoc [this key]
    (delete-file key folder))
  PBinaryAsyncKeyValueStore
  (-bget [this key locked-cb]
    (io-operation [key] folder config serializer read-handlers write-handlers buffer-size {:operation :read-binary
                                                                                    :config    config
                                                                                    :locked-cb locked-cb
                                                                                    :msg       {:type :read-binary-error
                                                                                                :key  key}}))
  (-bassoc [this key meta-up input]
    (io-operation [key] folder config serializer read-handlers write-handlers buffer-size {:operation  :write-binary
                                                                                    :input      input
                                                                                    :up-fn-meta meta-up
                                                                                    :config     config
                                                                                    :msg        {:type :write-binary-error
                                                                                                 :key  key}}))
  PKeyIterable
  (-keys [this]
    (list-keys folder serializer read-handlers)))

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
  [path  & {:keys [serializer read-handlers write-handlers buffer-size config]
            :or   {serializer     (ser/fressian-serializer)
                   read-handlers  (atom {})
                   write-handlers (atom {})
                   buffer-size    (* 1024 1024)
                   config         {:fsync true}}}]
  (let [_     (check-and-create-folder path)
        store (map->FileSystemStore {:folder         path
                                     :serializer     serializer
                                     :read-handlers  read-handlers
                                     :write-handlers write-handlers
                                     :buffer-size buffer-size    
                                     :locks          (atom {})
                                     :config         config})]
    (go store)))

(comment
  (do
    (delete-store "/tmp/konserve")
    (def store (<!! (new-fs-store "/tmp/konserve")))
    (<!! (-update-in store ["fooNew2"] (fn [old] {:format :edn :key "bar2"}) (fn [old] 0))))

  (<!! (-update-in store ["fooNew3"] (fn [old] {:format :edn :key "bar4"}) (fn [old] 0)))

  (<!! (-keys store))

  (<!! (-update-in store ["fooNew2"] (fn [old] {:format :edn :key "bar2"}) inc))

  (<!! (-get store "f"))

  (doseq [i (range 1 100)]
    (<!! (-update-in store ["fooNew2"] (fn [old] {:format :edn :key "bar2"}) (fn [old] i))))


  (<!! (-get store "fooNew2"))

  (def store (<!! (new-fs-store "/tmp/konserve")))

  (<!! (-get-meta store "fooNew2"))

  (<!! (-get-meta store :foobar9))

  (time (<!! (-bassoc store :foobar9 (fn [old] {:format :binary :key :binbar}) (FileInputStream. "pdfnewPdf.pdf"))))


  (<!! (-bget store :foobar9 (fn [{:keys [input-stream]}]
                             (go
                               (do
                                (io/copy input-stream (FileOutputStream. "pdfnewPdf.pdf")) 
                                 true)))))

  (<!! (-bassoc store :binaryfoo (fn [old] {:format :binary :key :binaryfoo}) (byte-array (range 10))))


  (<!! (-bassoc store :binaryfoo (fn [old] {:format :binary :key :binaryfoo}) (StringReader. "foo213")))

  (<!! (-bget store :binaryfoo (fn [{:keys [input-stream]}]
                                 (go
                                   (do
                                     (prn (slurp input-stream)) 
                                     true)))))

  (<!! (-get-meta store :binaryfoo))


  
)

