import React from 'react';
import { Row } from 'react-bootstrap';
import { connect } from 'react-redux';

import { increaseRow } from '../actions/uiActions';

class HorizontalGutter extends React.Component {
    state = { dragging: false };
    render() {
        return (
            <Row
                style={{ margin: '0px', backgroundColor: '#b3b3b3', height: '4px', width: '100%', cursor: 'row-resize' }}                
                draggable
                onMouseDown={e => {
                    this.setState({ dragging: true })                    
                }}
                onMouseUp={e => {
                    this.setState({ dragging: false })
                }}
                onDragStart={e => {
                    var img = new Image();
                    img.src = 'data:image/gif;base64,R0lGODlhAQABAIAAAAUEBAAAACwAAAAAAQABAAACAkQBADs=';
                    e.dataTransfer.setDragImage(img, 0, 0);
                    e.dataTransfer.setData("value", 4);
                }}
                onDrag={e => {                    
                    if (this.state.dragging) {                        
                        let newHeight = e.clientY / window.innerHeight * 100 - 10.8;                                                
                        if (newHeight > 15) {                            
                            this.props.increaseRow(newHeight - this.props.rowSize);
                        }
                    }
                }}
            ></Row>
        );
    }
}

const mapStateToProps = state => {
    return {
        rowSize: state.ui.rowSize
    };
};

export default connect(mapStateToProps, { increaseRow })(HorizontalGutter);
