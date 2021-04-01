import React from 'react';

class Gutter extends React.Component {
    state = { dragging: false, base: 0 };
    render() {

        let { onAdjust, currentSize, mode, min, max } = this.props;

        const isVertical = mode === "vertical";
        const className = isVertical ? "accent-border" : "gutter-horizontal";

        return (
            <div
                draggable
                style={{ ...this.props.style }}
                className={className}
                onMouseDown={e => {
                    let base = isVertical ? e.clientX : e.clientY;
                    this.setState({ dragging: true, base: base })
                }}
                onMouseUp={e => {
                    this.setState({ dragging: false })
                }}
                onDragStart={e => {
                    var img = new Image();
                    img.src = 'data:image/gif;base64,R0lGODlhAQABAIAAAAUEBAAAACwAAAAAAQABAAACAkQBADs=';
                    e.dataTransfer.setDragImage(img, 0, 0);
                }}
                onDrag={e => {
                    if (this.state.dragging) {
                        let diff = 0;
                        if (isVertical) {
                            diff = (e.clientX - this.state.base) / window.innerWidth * 100;
                        }
                        else {
                            diff = (e.clientY - this.state.base) / window.innerHeight * 100;   
                        }
                        let newSize = currentSize + diff;
                        let newBase = isVertical ? e.clientX : e.clientY;
                        if (newSize >= min && newSize <= max) {
                            onAdjust(newSize);
                            this.setState({ base: newBase })
                        }
                    }
                }}
            ></div>
        );
    }

}

export default Gutter;