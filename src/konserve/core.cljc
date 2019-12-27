(ns konserve.core
  (:refer-clojure :exclude [get get-in update update-in assoc assoc-in exists? dissoc keys])
  (:require [konserve.protocols :refer [-exists? -get-in -assoc-in
                                        -update-in -dissoc -bget -bassoc
                                        -keys]]
            [hasch.core :refer [uuid]]
            [clojure.test :refer [testing is]]
            #?(:cljs [cljs.nodejs :as node])
            #?(:clj [clojure.core.async :as async :refer [chan poll! put! <! go <!!]]
               :cljs [cljs.core.async :as async :refer (take! <! >! put! take! close! chan poll!)]))
  #?(:cljs (:require-macros [cljs.core.async.macros :refer [go]]
                            [cljs.test :refer [is testing]]
                            [konserve.core :refer [go-locked]])))


#?(:cljs (defonce fs (node/require "fs")))

#?(:cljs (defonce stream (node/require "stream")))

(defn- cljs-env?
  "Take the &env from a macro, and tell whether we are expanding into cljs."
  [env]
  (boolean (:ns env)))


#?(:clj
   (defmacro if-cljs
     "Return then if we are generating cljs code and else for Clojure code.
     https://groups.google.com/d/msg/clojurescript/iBY5HaQda4A/w1lAQi9_AwsJ"
     [then else]
     (if (cljs-env? &env) then else)))

;; TODO we keep one chan for each key in memory
;; as async ops seem to interfere with the atom state changes
;; and cause deadlock
(defn get-lock [{:keys [locks] :as store} key]
  (or (clojure.core/get @locks key)
      (let [c (chan)]
        (put! c :unlocked)
        (clojure.core/get (swap! locks (fn [old]
                                         (if (old key) old
                                             (clojure.core/assoc old key c))))
             key))))

#?(:clj
   (defmacro go-locked [store key & code]
     (let [res`(if-cljs
                (cljs.core.async.macros/go
                  (let [l# (get-lock ~store ~key)]
                    (try
                      (cljs.core.async/<! l#)
                      ~@code
                      (finally
                        (cljs.core.async/put! l# :unlocked)))))
                (go
                  (let [l# (get-lock ~store ~key)]
                    (try
                      (<! l#)
                      ~@code
                      (finally
                        (put! l# :unlocked))))))]
       res)))


(defn exists?
  "Checks whether value is in the store."
  [store key]
  (go-locked
   store key
   (<! (-exists? store key))))

(defn get-in
  "Returns the value stored described by key-vec. Returns nil if the key-vec is
   not present, or the not-found value if supplied."
  ([store key-vec]
   (get-in store key-vec nil))
  ([store key-vec not-found]
   (go-locked
    store (first key-vec)
    (let [a (<! (-get-in store [(first key-vec)]))]
     (if (some? a)
      (clojure.core/get-in a (rest key-vec))
      not-found)))))

(defn get
  "Returns the value stored described by key. Returns nil if the key
   is not present, or the not-found value if supplied."
  ([store key]
   (get-in store [key]))
  ([store key not-found]
   (get-in store [key] not-found)))

(defn update-in
  "Updates a position described by key-vec by applying up-fn and storing
  the result atomically. Returns a vector [old new] of the previous
  value and the result of applying up-fn (the newly stored value)."
  [store key-vec fn & args]
  (go-locked
   store (first key-vec)
   (<! (-update-in store key-vec fn args))))

(defn update
  "Updates a position described by key by applying up-fn and storing
  the result atomically. Returns a vector [old new] of the previous
  value and the result of applying up-fn (the newly stored value)."
  [store key fn & args]
  (apply update-in store [key] fn args))

(defn assoc-in
  "Associates the key-vec to the value, any missing collections for
  the key-vec (nested maps and vectors) are newly created."
  [store key-vec val]
  (go-locked
   store (first key-vec)
   (<! (-assoc-in store key-vec val))))

(defn assoc
 "Associates the key-vec to the value, any missing collections for
 the key-vec (nested maps and vectors) are newly created."
 [store key val]
 (assoc-in store [key] val))

(defn dissoc
  "Removes an entry from the store. "
  [store key]
  (go-locked
   store key
   (<! (-dissoc store key))))


(defn append
  "Append the Element to the log at the given key or create a new append log there.
  This operation only needs to write the element and pointer to disk and hence is useful in write-heavy situations."
  [store key elem]
  (go-locked
   store key
   (let [head (<! (-get-in store [key]))
         [append-log? last-id first-id] head
         new-elem {:next nil
                   :elem elem}
         id (uuid)]
     (when (and head (not= append-log? :append-log))
       (throw (ex-info "This is not an append-log." {:key key})))
     (<! (-update-in store [id] (fn [_] new-elem)))
     (when first-id
       (<! (-update-in store [last-id :next] (fn [_] id))))
     (<! (-update-in store [key] (fn [_] [:append-log id (or first-id id)])))
     [first-id id])))

(defn log
  "Loads the whole append log stored at "
  [store key]
  (go
   (let [head (<! (get-in store [key]));; atomic read
         [append-log? last-id first-id] head] 
     ;; all other values are immutable:
     (when (and head (not= append-log? :append-log))
       (throw (ex-info "This is not an append-log." {:key key})))
     (when first-id
       (loop [{:keys [next elem]} (<! (get-in store [first-id]))
              hist []]
         (if next
           (recur (<! (get-in store [next]))
                  (conj hist elem))
           (conj hist elem)))))))

(defn reduce-log
  "Loads the whole append log stored at "
  [store key reduce-fn acc]
  (go
   (let [head (<! (get-in store [key]));; atomic read
         [append-log? last-id first-id] head] 
     ;; all other values are immutable:
     (when (and head (not= append-log? :append-log))
       (throw (ex-info "This is not an append-log." {:key key})))
     (if first-id
       (loop [id first-id
              acc acc]
         (let [{:keys [next elem]} (<! (get-in store [id]))]
           (if (and next (not= id last-id))
             (recur next (reduce-fn acc elem))
             (reduce-fn acc elem))))
       acc))))


(defn bget 
  "Calls locked-cb with a platform specific binary representation inside
  the lock, e.g. wrapped InputStream on the JVM and Blob in
  JavaScript. You need to properly close/dispose the object when you
  are done!

  You have to do all work in a async thread of locked-cb, e.g.

  (fn [{is :input-stream}]
    (async/thread
      (let [tmp-file (io/file \"/tmp/my-private-copy\")]
        (io/copy is tmp-file))))
  "
  [store key locked-cb]
  (go-locked
   store key
   (<! (-bget store key locked-cb))))


(defn bassoc 
  "Copies given value (InputStream, Reader, File, byte[] or String on
  JVM, Blob in JavaScript) under key in the store."
  [store key val]
  (go-locked
   store key
   (<! (-bassoc store key val))))


(defn keys
  "Return a channel that will yield all top-level keys currently in the store."
  ([store] (-keys store)))

(defn compliance-test
  "Test the functionality of the Konserve Operations."
  [init-store delete-store list-keys]
  (testing "Core Konserve Functions"
    (go
       (let [folder "/tmp/konserve-test"
             _      (delete-store folder)
             store  (init-store folder)
             binbar (atom nil)]
         #_(<! (bassoc store :binbar #?(:clj (byte-array (range 10))
                                      :cljs (js/Buffer.from (clj->js (range 10))))))
         #_(<! (bget store :binbar
                   #?(:clj (fn [{:keys [input-stream]}]
                             (go
                               (reset! binbar (map byte (slurp input-stream)))))
                      :cljs (let [ch (chan)
                                  rs (:read-stream %)]
                              (.on rs "data" (fn [chunk]
                                               (let [x chunk]
                                                 (reset! binbar x))))
                              (.on rs "close" (fn [_]
                                                (put! ch true)
                                                (close! ch)))
                              (.on rs "error" (fn [err] (prn err)))
                              ch))))
         (<! (assoc store :foo :bar))
         (is (= (<! (get store :foo))
                :bar))
         (is (= (<! (get-in store [:foo]))
                :bar))
         (<! (assoc-in store [:foo] {:bar :baz}))
         (is (= (<! (get-in store [:foo :bar]))
                :baz))
         (<! (assoc store :baz :bar))
         (<! (update store :baz name))
         (is (= "bar"
                (<! (get store :baz))))
         (<! (assoc-in store [:baz] {:bar 42}))
         (is (= (<! (get-in store [:baz :bar]))
                42))
         (<! (update-in store [:baz :bar] inc))
         #?(:clj (is (= (<! (get-in store [:baz :bar]))
                   43)))
         (<! (update-in store [:baz :bar] + 2 3))
         #?(:clj (is (= (<! (get-in store [:baz :bar]))
                        48)))
         #_(is (= (<!! (list-keys store))
                  #{{:key :foo, :format :edn} {:key :binbar, :format :binary}}))
         (<! (dissoc store :foo))
         (is (= (<! (get-in store [:foo]))
                nil))
         #_(is (= (<!! (list-keys store))
                  #{{:key :binbar, :format :binary}}))
         #_(is #?(:clj (= @binbar (range 10))
                :cljs (= (.toString @binbar) (.toString (js/Buffer.from (clj->js (range 10)))))))
         #_(is (= [:binbar] (<!! (async/into [] (keys store)))))
         (delete-store folder)
         #_(let [store (init-store folder)]
             (is (= (<!! (list-keys store))
                    #{})))))))


(comment
  (require '[clojure.core.async :refer [<!! chan go] :as async])
  ;; cljs
  (go (def store (<! (new-indexeddb-store "konserve" #_(atom {'konserve.indexeddb.Test
                                                               map->Test})))))




  ;; clj
  (require '[konserve.filestore :refer [new-fs-store list-keys delete-store]]
           '[konserve.memory :refer [new-mem-store]]
           '[clojure.core.async :refer [<!! >!! chan] :as async])

  (def store (<!! (new-fs-store "/tmp/store")))

  (doseq [i (range 1000)]
    (<!! (update-in store [:foo] (fn [j] (if-not j i (+ j i))))))

  (<!! (get-in store [:foo]))

  (delete-store "/tmp/store")

  (def store (<!! (new-mem-store)))

  (<!! (list-keys store))

  (<!! (get-lock store :foo))
  (put! (get-lock store :foo) :unlocked)

  (<!! (append store :foo :barss))
  (<!! (log store :foo))
  (<!! (get-in store [:foo]))
  (<!! (get-in store []))

  (let [numbers (doall (range 1024))]
    (time
     (doseq [i (range 1000)]
       (<!! (assoc-in store [i] numbers)))))
  ;; fs-store: ~7.2 secs on my old laptop
  ;; mem-store: ~0.186 secs

  (let [numbers (doall (range (* 1024 1024)))]
    (time
     (doseq [i (range 10)]
       (<!! (assoc-in store [i] numbers)))))
  ;; fs-store: ~46 secs, large files: 1 million ints each
  ;; mem-store: ~0.003 secs


  ;; check lock retaining:
  (let [numbers (range (* 10 1024 1024))]
    (assoc-in store [42] numbers))

  (get-lock store 42)


  (<!! (log store :bar))

  (<!! (assoc-in store [{:nanofoo :bar}] :foo))

  ;; investigate https://github.com/stuartsierra/parallel-async
  (let [res (chan (async/sliding-buffer 1))
        v (vec (range 5000))]
    (time (->>  (range 5000)
                (map #(assoc-in store [%] v))
                async/merge
                #_(async/pipeline-blocking 4 res identity))))
                 ;; 38 secs
  (go (println "2000" (<! (get-in store [2000]))))

  (let [res (chan (async/sliding-buffer 1))
        ba (byte-array (* 10 1024) (byte 42))]
    (time (->>  (range 10000)
                (map #(-bassoc store % ba))
                async/merge
                (async/pipeline-blocking 4 res identity)
                #_(async/into [])
                <!!))) ;; 19 secs


  (let [v (vec (range 5000))]
    (time (doseq [i (range 10000)]
            (<!! (-assoc-in store [i] i))))) ;; 19 secs

  (time (doseq [i (range 10000)]
          (<!! (-get-in store [i])))) ;; 2706 msecs

  (<!! (-get-in store [11]))

  (<!! (-assoc-in store ["foo"] nil))
  (<!! (-assoc-in store ["foo"] {:bar {:foo "baz"}}))
  (<!! (-assoc-in store ["foo"] (into {} (map vec (partition 2 (range 1000))))))
  (<!! (update-in store ["foo" :bar :foo] #(str % "foo")))
  (type (<!! (get-in store ["foo"])))

  (<!! (-assoc-in store ["baz"] #{1 2 3}))
  (<!! (-assoc-in store ["baz"] (java.util.HashSet. #{1 2 3})))
  (type (<!! (-get-in store ["baz"])))

  (<!! (-assoc-in store ["bar"] (range 10)))
  (.read (<!! (-bget store "bar" :input-stream)))
  (<!! (-update-in store ["bar"] #(conj % 42)))
  (type (<!! (-get-in store ["bar"])))

  (<!! (-assoc-in store ["boz"] [(vec (range 10))]))
  (<!! (-get-in store ["boz"]))



  (<!! (-assoc-in store [:bar] 42))
  (<!! (-update-in store [:bar] inc))
  (<!! (-get-in store [:bar]))

  (import [java.io ByteArrayInputStream ByteArrayOutputStream])
  (let [ba (byte-array (* 10 1024 1024) (byte 42))
        is (io/input-stream ba)]
    (time (<!! (-bassoc store "banana" is))))
  (def foo (<!! (-bget store "banana" identity)))
  (let [ba (ByteArrayOutputStream.)]
    (io/copy (io/input-stream (:input-stream foo)) ba)
    (alength (.toByteArray ba)))

  (<!! (-assoc-in store ["monkey" :bar] (int-array (* 10 1024 1024) (int 42))))
  (<!! (-get-in store ["monkey"]))

  (<!! (-assoc-in store [:bar/foo] 42))

  (defrecord Test [a])
  (<!! (-assoc-in store [42] (Test. 5)))
  (<!! (-get-in store [42]))



  (assoc-in nil [] {:bar "baz"})





  (defrecord Test [t])

  (require '[clojure.java.io :as io])

  (def fsstore (io/file "resources/fsstore-test"))

  (.mkdir fsstore)

  (require '[clojure.reflect :refer [reflect]])
  (require '[clojure.pprint :refer [pprint]])
  (require '[clojure.edn :as edn])

  (import '[java.nio.channels FileLock]
          '[java.nio ByteBuffer]
          '[java.io RandomAccessFile PushbackReader])

  (pprint (reflect fsstore))


  (defn locked-access [f trans-fn]
    (let [raf (RandomAccessFile. f "rw")
          fc (.getChannel raf)
          l (.lock fc)
          res (trans-fn fc)]
      (.release l)
      res))


  ;; doesn't really lock on quick linux check with outside access
  (locked-access (io/file "/tmp/lock2")
                 (fn [fc]
                   (let [ba (byte-array 1024)
                         bf (ByteBuffer/wrap ba)]
                     (Thread/sleep (* 60 1000))
                     (.read fc bf)
                     (String. (java.util.Arrays/copyOf ba (.position bf))))))


  (.createNewFile (io/file "/tmp/lock2"))
  (.renameTo (io/file "/tmp/lock2") (io/file "/tmp/lock-test"))


  (.start (Thread. (fn []
                     (locking "foo"
                       (println "locking foo and sleeping...")
                       (Thread/sleep (* 60 1000))))))

  (locking "foo"
    (println "another lock on foo"))

  (time (doseq [i (range 10000)]
          (spit (str "/tmp/store/" i) (pr-str (range i))))))

