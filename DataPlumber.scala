trait DataPlumber[S, D] {
    def source: DataSource[S]
    def sink: DataSink[D]
    final def run = sink.write(transform(source.read))

    def transform(rows: List[S]): List[D]

}

trait DataSource[S] {
    def read: List[S]
}

trait DataSink[D] {
    def write(rows: List[D]): Unit
}