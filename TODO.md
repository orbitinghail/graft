The RequestGroup/RequestGroupAggregate is the wrong abstraction.

It's not super clear yet what the best way to communicate from the uploader back to the frontend is. But it will certainly be made easier if we transition to a batch/websocket model.

Once we are in a batch mode, we can create a single response channel to receive commits. The channel can be passed over to the writer/uploader along with our vid and the pages.

When the uploader finishes a segment, it iterates through relevant channels, sending the corresponding offset sets and segment ids back to the frontend.

For now, we will use croaring to handle offset sets. croaring supports run container optimization and in general is fairly efficient. Make sure we optimize the sets before serialization. In theory, we can build offset sets while we serialize the segment as all the corresponding details are there.

This approach has some overhead in allocating new channels for each batch request, however that is amortized by the batch size.