(ns nucalc.serialization
  (:require [clojure.java.io :as io]
            [taoensso.nippy :as nippy])
  (:import [java.io DataInputStream DataOutputStream EOFException]))

(defn with-data-output-stream [file-name function]
  (with-open [data-output-stream (DataOutputStream. (io/output-stream (io/file file-name)))]
    (function data-output-stream)))

(defn write-to-data-output-stream [data-output-stream sequence]
  (let [byte-array (nippy/freeze sequence)
        length (alength byte-array)]
    (.writeInt data-output-stream length)
    (.write data-output-stream byte-array 0 length)))

(defn write-file
  ([file-name sequence]
   (write-file file-name 1000 sequence))
  ([file-name partition-size sequence]
   (with-data-output-stream file-name
     (fn [data-output-stream]
       (doseq [batch (partition-all partition-size sequence)]
         (write-to-data-output-stream data-output-stream
                                      batch))))))

(defn transduce-file [file-name & {:as options}]
  (let [transducer (comp cat (or (:transducer options)
                                 identity))
        reducing-function (transducer (or (:reducer options)
                                          (constantly nil)))]
    (with-open [data-input-stream (DataInputStream. (io/input-stream (io/file file-name)))]

      (loop [value (if (contains? options :initial-value)
                     (:initial-value options)
                     (if (contains? options :reducer)
                       ((:reducer options))
                       nil))
             buffer nil]

        (if-let [segment-length (try (.readInt data-input-stream)
                                     (catch EOFException e))]
          (let [buffer (if (and buffer
                                (< segment-length (alength ^bytes buffer)))
                         buffer
                         (byte-array (int (* 1.3 segment-length))))]

            (.read data-input-stream buffer 0 segment-length)
            (let [result (reducing-function value (nippy/thaw buffer))]
              (if (reduced? result)
                (reducing-function @result)
                (recur result buffer))))
          (reducing-function value))))))

(comment
  (write-file "temp/test.data" 10 (range 100))
  (= (range 100)
     (transduce-file "temp/test.data" :reducer conj))
  )
