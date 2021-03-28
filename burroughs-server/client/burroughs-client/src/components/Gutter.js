import React from 'react';

import { connect } from 'react-redux';
import { increaseCol } from '../actions/uiActions';


class Gutter extends React.Component {
    state = { dragging: false };
    render() {
        return (
            <div
                draggable
                style={{ ...this.props.style }} className="accent-border"
                onMouseDown={e => {                    
                    this.setState({ dragging: true})
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
                        let newWidth = e.clientX / window.innerWidth * 100;
                        if (newWidth > 15) {                            
                            this.props.increaseCol(newWidth - this.props.columnSizes[0]);
                        }
                    }
                }}
            ></div>
        );
    }

}
const mapStateToProps = state => {
    return {
        columnSizes: state.ui.columnSizes
    };
};

export default connect(mapStateToProps, { increaseCol })(Gutter);